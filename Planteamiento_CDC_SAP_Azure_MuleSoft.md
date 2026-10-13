# Artes Gráficas — Change Data Capture de SAP a Azure Data Lake con MuleSoft

**Planteamiento de solución** · Versión 1.0 · 13-oct-2026

---

## 1. Resumen ejecutivo

Artes Gráficas necesita mover de forma continua los cambios de su ERP SAP (ECC sobre HANA) hacia un Data Lake en Azure (ADLS Gen2). Requiere capturar inserciones, modificaciones y borrados (CDC), sin perder ni duplicar información y con trazabilidad completa.

La propuesta usa **MuleSoft Anypoint Platform sobre CloudHub 2.0**. Se basa en una arquitectura **API-led y orientada a eventos**:

- **Conectores de captura intercambiables:** IDoc, ODP/OData, triggers de HANA y RFC. Todos publican cada cambio en un **sobre CDC común** hacia **Anypoint MQ**.
- **Una aplicación de entrega:** escribe en el lago con **commit atómico y manifiesto**.
- **Una API de control:** cubre la auditoría, la conciliación y el reproceso.

| Capacidad | Cómo la resuelve MuleSoft |
|---|---|
| **Movimiento de datos** | SAP Connector (IDoc/RFC), HTTP/OData (ODP), Database Connector (HANA), DataWeave para la transformación y HTTP hacia ADLS Gen2 |
| **Entrega asegurada** | Anypoint MQ con ACK manual, cola de mensajes fallidos (DLQ), validación de idempotencia y commit en orden. Garantía: *al menos una vez* + idempotencia = *efectivamente una vez* |
| **Reintentos** | 4 niveles: reconexión de conectores → `until-successful` selectivo → reentrega de MQ → reproceso de la DLQ desde la API |
| **Transaccionalidad** | Transacciones locales y XA en base de datos y VM; escritura atómica en ADLS con `_tmp` + rename + manifiesto; la marca de agua se confirma solo después de entregar |
| **Trazabilidad** | Un correlationId de punta a punta, logs JSON por etapa, auditoría consultable por cambio, conciliación diaria SAP ↔ lago y Anypoint Monitoring con alertas |

---

## 2. Contexto y volumetría

Alcance inicial: **63 tablas SAP**, unos **75 GB** almacenados. Fuente: *Volumetría_TablasDatosMaestrosSAP.xlsx*.

| Indicador | Valor |
|---|---|
| Cambios en 2026 (1-ene a 13-oct, creados + modificados) | ~3.0 millones |
| **Cambios por día** | ~10,500 en días naturales · ~15,000 en días hábiles |
| Pico estimado en una jornada de 10 horas | ~1,500 cambios por hora (~25 por minuto) |
| Volumen diario a mover | ~15–22 MB por día (almacenamiento HANA) · ~50–70 MB por día en JSON |
| Dimensionamiento para el cierre de mes (×5) | ~75,000 cambios por día |
| Volumen histórico (carga inicial, una sola vez) | ~75 GB |

**Tablas con más cambios por día hábil:**

| Tabla | Descripción | Cambios/día hábil | Tamaño |
|---|---|---|---|
| GLPCA | Contabilidad de centros de beneficio | ~4,150 | 20.4 GB |
| BSIS | Partidas individuales de mayor | ~2,950 | 10.5 GB |
| MSEG | Movimientos de mercancía | ~1,570 | 12.3 GB |
| COSP | Costos CO plan/real | ~1,360 | 8.0 GB |
| BKPF | Cabecera de documento contable | ~1,240 | 7.5 GB |

**Conclusión:** la operación diaria del CDC es ligera y el único desafío de volumen es la **carga inicial**. Por eso se pueden ofrecer latencias cortas sin presionar a SAP y con un consumo de vCores bajo.

> **Hallazgo técnico.** Las tablas VBUP, BSIS/BSID/BSAK y GLPCA tienen datos físicos. Esto indica **ECC sobre HANA (Suite on HANA)** y no S/4HANA, donde esas tablas son vistas de compatibilidad sobre ACDOCA/MATDOC o ya no existen. El diseño parte de ECC sobre HANA y queda preparado para una migración futura a S/4HANA, en la que bastaría cambiar los conectores de captura por CDS con delta.

---

## 3. Objetivos y criterios de éxito

1. **Cero pérdidas:** cada cambio confirmado en SAP aparece en el lago, lo que se demuestra con la conciliación diaria.
2. **Cero duplicados efectivos:** quien consuma el lago ve una sola versión vigente por llave.
3. **Latencia por dominio** (ver §6) cumplida en el percentil 95.
4. **Trazabilidad:** cualquier cambio se localiza por su correlationId en menos de 1 minuto (`GET /trace/{id}`).
5. **Recuperación automática** de fallas transitorias (red, SAP o Azure) sin intervención de nadie.
6. **Independencia del motor de consumo:** la capa bronze es neutral y sirve para Databricks, Fabric, Synapse o ADF.

---

## 4. Supuestos y validaciones pendientes

| # | Tema | Supuesto | Impacto si no se cumple |
|---|---|---|---|
| V1 | **Licencia HANA** | Puede ser *enterprise* (opción 1) o *runtime* (opción 2) | Con licencia runtime **no se permite SQL directo**: se descarta la captura por triggers y todo se resuelve con ODP, IDoc y RFC |
| V2 | SAP Gateway / OData | Disponible para exponer ODP vía OData (SEGW / RSO2) | Sin Gateway, la captura ODP se cambia por RFC con un BAPI Z de delta |
| V3 | Restricción de ODP vía RFC | Se respeta la **nota SAP 3255746**, que prohíbe usar ODP por RFC desde herramientas de terceros | Por eso se elige **ODP vía OData**, que es el canal permitido |
| V4 | Conectividad | VPN entre la red de SAP y el **Private Space** de CloudHub 2.0 | Se requiere un runtime híbrido on-premise |
| V5 | Change pointers | Activables para DEBMAS, CREMAS, MATMAS, COSMAS y COND_A (BD50/BD52/BD61) | Los maestros pasan a ODP |
| V6 | Lago | ADLS Gen2 con **espacio jerárquico** habilitado y un Service Principal con el rol *Storage Blob Data Contributor* | Sin espacio jerárquico, el rename no es atómico y el manifiesto se vuelve la única garantía |

---

## 5. Arquitectura

```
 SAP ECC on HANA                     CloudHub 2.0 (Private Space)                         Azure
┌──────────────┐  IDoc   ┌───────────────────┐
│ Change ptrs  │────────▶│ sap-idoc-cdc      │──┐
├──────────────┤  OData  ├───────────────────┤  │   ┌──────────────────┐   ┌──────────────────────┐
│ ODP / ODQ    │────────▶│ sap-odp-cdc       │──┼──▶│ Anypoint MQ      │──▶│ adls-delivery        │──▶ ADLS Gen2
├──────────────┤  SQL    ├───────────────────┤  │   │ exch: cdc.events │   │ micro-batch + commit │    bronze/sap/<tabla>/
│ ZCDC_LOG (*) │────────▶│ hana-trigger-cdc  │──┤   │ q por dominio    │   │ atómico + manifest   │    yyyy/mm/dd/<batch>
├──────────────┤  RFC    ├───────────────────┤  │   │ + DLQ            │   └─────────┬────────────┘
│ Catálogos    │────────▶│ sap-rfc-snapshot  │──┘   └──────────────────┘             │
└──────────────┘         └───────────────────┘                                       ▼
                     watermarks → Object Store v2          cdc-control-api: auditoría, conciliación,
                     (*) solo con licencia enterprise      reproceso DLQ, re-sync por tabla
```

### 5.1 Componentes

| Aplicación | Capa | Responsabilidad | Tamaño (CH2) |
|---|---|---|---|
| `sap-idoc-cdc` | System | Escucha IDocs de maestros (tRFC/qRFC), los transforma al sobre CDC y los publica | 0.2 vCore |
| `sap-odp-cdc` | System | Consulta los deltas ODP vía OData por suscripción y guarda el delta token | 0.2 vCore |
| `hana-trigger-cdc` | System | *(Opción 1)* Lee `ZCDC_LOG` y marca las filas como procesadas en una transacción local | 0.1 vCore |
| `sap-rfc-snapshot` | System | Hace la carga completa diaria de catálogos vía RFC (`op = R`) | 0.1 vCore |
| `adls-delivery` | Process | Consume de MQ, valida idempotencia, arma micro-lotes, escribe con commit atómico y confirma (ACK) | 0.2 vCore × 2 réplicas |
| `cdc-control-api` | Experience | Expone `/trace`, `/reconcile`, `/dlq/replay`, `/resync` y `/health/lag` | 0.1 vCore |
| `sap-initial-load` | Batch | Carga histórica particionada; se apaga al terminar | 1 vCore (temporal) |

**Consumo permanente: ~1.1 vCores.** La carga inicial suma 1 vCore solo mientras dura.

### 5.2 El sobre CDC (contrato canónico)

Todas las fuentes publican el mismo contrato. Por eso los mecanismos de captura son **intercambiables** y la entrega no sabe de dónde viene el dato.

```json
{
  "cdcId": "6f1c2a7e-…",
  "correlationId": "b1e0…",
  "source": { "system": "ECC_PRD", "client": "300", "mechanism": "ODP", "table": "VBAP" },
  "op": "U",
  "key": { "VBELN": "0000123456", "POSNR": "000010" },
  "changeTs": "2026-10-13T15:02:11Z",
  "seq": 884512,
  "watermark": "ODQ:20261013150000.000123",
  "before": null,
  "after": { "MATNR": "PAP-COUCHE-150", "KWMENG": 1200, "NETWR": 18450.00 }
}
```

| Campo | Uso |
|---|---|
| `op` | `I` inserción · `U` modificación · `D` borrado · `R` refresh (carga completa o inicial) |
| `key` | Llave de negocio de SAP, que sirve para el MERGE en el lago |
| `seq` | Orden monótono por fuente; quien consume aplica *gana el último* |
| `watermark` | Posición en la fuente; se confirma solo después del commit en ADLS |
| `correlationId` | Hilo de trazabilidad entre SAP, Mule, el lago y la auditoría |

### 5.3 Capa bronze en ADLS Gen2

```
abfss://datalake@agstorage.dfs.core.windows.net/
├── bronze/sap/ecc_prd/<tabla>/yyyy=2026/mm=10/dd=13/<batchId>.jsonl
├── _manifest/<tabla>/yyyy=2026/mm=10/dd=13/<batchId>.json
├── _tmp/                       ← escritura en curso (nunca se lee)
└── _audit/reconciliation/<fecha>.json
```

**Regla para quien consuma:** *solo se procesan archivos que tengan manifiesto.*

Ejemplo de referencia con Databricks: Auto Loader sobre `_manifest/`, y luego `MERGE INTO silver.<tabla> USING … ON key WHEN MATCHED AND s.seq > t.seq …`, donde `op = D` se traduce en un borrado.

---

## 6. Estrategia de captura por dominio

| Dominio | Mecanismo | Latencia | Tablas |
|---|---|---|---|
| **Maestros** | IDoc con change pointers (DEBMAS, CREMAS, MATMAS, COSMAS, GLMAST, COND_A) | Por evento, menos de 5 min | KNA1, KNVV, KNVP, ADR6, LFA1, MARA, MAKT, MARM, MVKE, MBEW, CSKS, CSKT, SKAT, KONP, A305 |
| **Ventas, compras y producción** | ODP vía OData con delta | Cada 5 min | VBAK, VBAP, VBEP, VBKD, VBUP, VBRK, VBRP, EKKO, EKPO, EKET, AUFK, AFKO, AFPO, RESB, MSKA, JEST, ZSD_CANC_PEDIDO |
| **Finanzas, costos e inventario** | ODP con delta; triggers de HANA si es la opción 1 | Cada 15 min | BKPF, BSIS, BSID, BSAD, BSIK, BSAK, FAGLFLEXT, GLPCA, COSP, MSEG, MKPF, S031, S032, CDHDR |
| **Catálogos** | RFC con carga completa | Diario, 02:00 | T001, T001W, T001L, T006A, T134T, T151T, T178T, TVM1T–TVM4T, TCURR, TVKOT, TVTWT, TSPAT, TVGRT, TVAGT |

**Consideraciones de experto:**

- **Extractores estándar vs. réplica 1:1 de tabla.** Los extractores de contenido estándar (2LIS_11_*, 2LIS_02_*, 2LIS_03_BF, 0FI_GL_14, 0FI_AR_4, 0FI_AP_4, 0EC_PCA_3, 0CO_OM_CCA_1) entregan una vista de negocio, no la tabla tal cual. Para una réplica 1:1 se crean **extractores genéricos (RSO2)** con delta por puntero numérico o timestamp, y se exponen vía OData.
- **Partidas abiertas → compensadas.** Al compensar una partida, SAP la **borra** de BSID/BSIK y la **inserta** en BSAD/BSAK. El CDC debe emitir `D` + `I`. Los triggers lo capturan de forma nativa; con ODP se usa un extractor que informa el cambio de estado.
- **Tablas sin delta confiable** (JEST, RESB, ZSD_CANC_PEDIDO): con la opción 1 se usan triggers; con la opción 2, un extractor genérico guiado por CDHDR/CDPOS más una re-sincronización semanal.

---

## 7. Garantías de entrega

### 7.1 Entrega garantizada: ningún paso confirma antes que el siguiente

| Tramo | Garantía | Qué pasa si falla |
|---|---|---|
| SAP → `sap-idoc-cdc` | Listener transaccional tRFC/qRFC; la confirmación a SAP sale solo después de publicar en MQ | El IDoc queda en SM58/BD87 y SAP lo reintenta |
| SAP → `sap-odp-cdc` | El delta token se guarda solo cuando **todas** las páginas ya se publicaron | Se repite el mismo delta link y ODQ lo trata como repetición |
| `ZCDC_LOG` → `hana-trigger-cdc` | Se publica y luego se marca `PROCESSED` en una transacción local de base de datos | Rollback y relectura; los duplicados los filtra la idempotencia |
| MQ → `adls-delivery` | ACK **manual** solo después del commit en ADLS | NACK → reentrega; después de 5 intentos va a la **DLQ** |

> Anypoint MQ no participa en transacciones XA (2PC). La transaccionalidad de punta a punta se logra con **ACK manual + commit en orden + idempotencia**, que es el patrón estándar en sistemas distribuidos. Las transacciones locales y XA de Mule se usan donde aportan: en la base de datos y en colas VM transaccionales dentro de cada aplicación.

### 7.2 Commit atómico en ADLS Gen2

1. El *Aggregators module* agrupa hasta 5,000 registros o 60 s y conserva los `ackToken` de cada mensaje.
2. `PUT ?resource=file` → `PATCH ?action=append` → `PATCH ?action=flush`, escribiendo en `_tmp/<batchId>.jsonl`.
3. `PUT` con `x-ms-rename-source` hacia la ruta final en bronze. Con espacio jerárquico, este rename es **atómico**.
4. Se escribe `_manifest/<batchId>.json` con el conteo, el seq mínimo y máximo, el SHA-256 y el correlationId.
5. `anypoint-mq:ack` de todos los tokens del lote.

### 7.3 Reintentos en 4 niveles

| Nivel | Mecanismo | Alcance |
|---|---|---|
| 1 | Estrategia de **reconexión** de los conectores (SAP, base de datos, HTTP) | Caídas de VPN, reinicios de SAP |
| 2 | **`until-successful`** selectivo | Solo `HTTP:TIMEOUT`, `HTTP:CONNECTIVITY`, `429` y `503`. Los errores 4xx y de transformación **no** se reintentan |
| 3 | **Reentrega de Anypoint MQ** (`maxDeliveries=5`) → DLQ | Fallas que persisten |
| 4 | **`POST /dlq/{queue}/replay`** en `cdc-control-api` | Reproceso operado por una persona, después de corregir la causa |

Cada mensaje que cae en la DLQ lleva un **sobre de error** con `errorType`, `description`, `attempts`, `firstFailureTs` y el payload original.

### 7.4 Idempotencia y orden

- **`cdcId` determinista**: es un hash de la posición en la fuente (delta link + página + fila, DOCNUM + segmento, SEQ del log), así que una reentrega o una reextracción de SAP conserva el mismo id.
- El `cdcId` se registra en Object Store v2 (TTL de 7 días) **solo después del commit en ADLS**. Al recibir un mensaje únicamente se consulta. Por eso una reentrega se descarta solo si el cambio ya está en el lake; si no llegó a comprometerse, se procesa de nuevo y no se pierde.
- El transporte **no exige orden**. El `seq` + MERGE *gana el último* lo resuelve en el lago, así que no hacen falta colas FIFO, que limitan el throughput. El único orden estricto es el de los maestros, vía qRFC.

---

## 8. Trazabilidad y operación

- **Logs estructurados en JSON** por etapa: `CAPTURED → PUBLISHED → LANDED → COMMITTED` (o `DLQ`), con `correlationId`, `table`, `op` y `latencyMs`.
- **Auditoría:** cada etapa publica en `cdc.audit`; `cdc-control-api` lo guarda en `CDC_AUDIT` (Azure SQL).
- **API de control:**

| Endpoint | Propósito |
|---|---|
| `GET /trace/{correlationId}` | Recorrido completo de un cambio |
| `GET /health/lag` | Rezago (`now − changeTs`) por dominio contra su SLA |
| `POST /reconcile?table=&date=` | Compara el conteo de SAP (RFC) contra los manifiestos del lago |
| `POST /resync` | Recarga acotada por tabla, sociedad y fecha (`op = R`) |
| `POST /dlq/{queue}/replay` | Reproceso de la DLQ |

- **Conciliación diaria a las 03:00:** compara SAP contra el lago por tabla y fecha. Si hay diferencias, lanza una re-sincronización automática acotada y envía una alerta.
- **Anypoint Monitoring:**
  - Dashboards de throughput, rezago, profundidad de la DLQ y errores por tipo.
  - Alertas si la DLQ tiene más de 0 mensajes, si el rezago supera el SLA o si hay CPU o memoria altas.

---

## 9. Carga inicial (~75 GB)

- Aplicación aparte, `sap-initial-load`, con **Batch Job** particionado por **tabla × sociedad (BUKRS) × año (GJAHR)**.
- Las ventanas nocturnas y de fin de semana, junto con un límite de llamadas concurrentes a SAP, protegen la operación.
- Cada partición registra su estado en Object Store (`PENDING/RUNNING/DONE/FAILED`), así que la carga **se reanuda donde se quedó**.
- **Paso a delta sin huecos:**
  1. Se activa el CDC y se toma la marca de agua T0; MQ retiene los eventos.
  2. Corre la carga inicial hasta T0.
  3. Se libera el consumo del delta; el `seq` y el MERGE resuelven los traslapes.
  4. La conciliación confirma el resultado.

---

## 10. Seguridad

- **SAP:** usuario técnico RFC con roles mínimos (S_RFC, S_IDOCDEFT y autorización de lectura ODQ); SNC opcional.
- **Azure:** OAuth 2.0 client credentials con Entra ID (`scope = https://storage.azure.com/.default`) y RBAC *Storage Blob Data Contributor* acotado al contenedor.
- **Mule:** secure properties cifradas (AES); secretos en el Secrets Manager de Anypoint; TLS 1.2+; la API de control queda protegida con políticas de API Manager (Client ID Enforcement + OAuth).
- **Red:** Private Space con VPN hacia SAP y Private Endpoint hacia ADLS.

---

## 11. Plan de implementación sugerido

| Fase | Contenido | Resultado |
|---|---|---|
| **F0 – Validación** (2 sem) | Validaciones V1–V6, licencia HANA, Gateway, VPN | Diseño confirmado |
| **F1 – Piloto** (4 sem) | Maestros (KNA1, MARA) + VBAK/VBAP + GLPCA, de punta a punta con DLQ, trazabilidad y conciliación | Demostración de garantías con datos reales |
| **F2 – Escalamiento** (4 sem) | Resto de tablas por dominio, dashboards y alertas | 63 tablas en CDC |
| **F3 – Carga inicial y salida a producción** (3 sem) | Carga histórica, paso a delta, hipercuidado | Operación productiva |

---

## 12. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| La licencia runtime impide el SQL directo | Arquitectura de captura intercambiable; ODP/IDoc/RFC cubren el 100% |
| Pico en el cierre de mes | Dimensionado ×5; `adls-delivery` escala horizontalmente con réplicas |
| Tablas sin delta confiable | Triggers (opción 1) o CDHDR + re-sincronización semanal |
| La carga inicial afecta a SAP | Ventanas, límite de concurrencia y reanudación por partición |
| Throttling de ADLS (429/503) | `until-successful` + micro-lotes + reentrega de MQ |
| Migración futura a S/4HANA | Solo se cambian los conectores de captura (CDS con delta); entrega, lago y operación no cambian |

---

## 13. ¿Por qué MuleSoft?

- **Un solo runtime** para IDoc, RFC, OData, SQL, MQ y REST de Azure, en vez de varias herramientas separadas.
- **Las garantías son configuración, no código a la medida:** reconexión, `until-successful`, ACK manual, DLQ, idempotencia y Object Store.
- **Reutilización por capas (API-led):** el sobre CDC y la API de control sirven también para futuras fuentes (Salesforce, WMS, MES).
- **Costo bajo en operación diaria:** ~1.1 vCores para ~15,000 cambios diarios.
- **Operación visible:** Anypoint Monitoring, API Manager y Runtime Manager en una sola consola.
