# Artes Gráficas — CDC SAP → Azure Data Lake con MuleSoft (código de referencia)

Implementación de referencia del [planteamiento](../Planteamiento_CDC_SAP_Azure_MuleSoft.md).
Mule 4.11.2 · Java 17 · CloudHub 2.0 · conectores resueltos desde Anypoint Exchange.

## Mapa de proyectos

| Proyecto | Capa | Qué demuestra | Disparador |
|---|---|---|---|
| `cdc-common/` | Contrato | Sobre CDC (JSON Schema) + módulo DataWeave `dw::cdc::Envelope` compartido | — |
| `sap-idoc-cdc/` | System | Maestros por **IDoc** (DEBMAS/CREMAS/MATMAS) con **transacción tRFC**: si falla, el IDoc queda en SM58 | `sap:document-listener` |
| `sap-odp-cdc/` | System | Deltas **ODP vía OData**; el delta token solo avanza tras publicar todas las páginas | `scheduler` 5 / 15 min |
| `hana-trigger-cdc/` | System | Triggers ligeros de HANA → `ZCDC_LOG`; lectura `STATUS='NEW'` con **transacción local por fila** (captura borrados) | `scheduler` 1 min |
| `adls-delivery/` | Process | **ACK manual**, idempotencia, micro-lotes, **commit atómico** `_tmp → rename → _manifest`, reintento selectivo, DLQ | `anypoint-mq:subscriber` |
| `cdc-control-api/` | Experience | `/trace`, `/health/lag`, `/reconcile`, `/resync`, `/dlq/replay` + auditoría en Azure SQL | `http:listener` + APIkit |
| `sap-initial-load/` | Batch | Carga histórica con **Batch Job** reanudable por partición + recargas `/resync` | `scheduler` cron nocturno |

`sap-rfc-snapshot` (catálogos diarios) no se incluye: es una variante trivial de `read-partition-page-flow` de
`sap-initial-load` con `op = R` y un cron diario a las 02:00.

## Garantías y dónde están en el código

| Garantía | Implementación |
|---|---|
| Entrega asegurada | `adls-delivery.xml` — subscriber `acknowledgementMode="MANUAL"`; `anypoint-mq:ack` solo después de `adls-commit-sub-flow` |
| Idempotencia | `cdcId` determinista (`cdcIdOf` en `Envelope.dwl`); `os:contains` al recibir y `os:store` **solo tras el commit** en ADLS (TTL 7 días) |
| Commit por tabla | Cada tabla del micro-lote es una unidad independiente: commit → registro de cdcIds → ACK; falla → NACK o DLQ solo de esa tabla |
| Commit atómico | `adls-write-tmp-sub-flow` → `adls-rename-sub-flow` (rename idempotente) → `adls-write-manifest-sub-flow` |
| Reintento nivel 1 | `<reconnection>` en las conexiones de SAP, Anypoint MQ y ADLS |
| Reintento nivel 2 | `until-successful` + `adls-selective-retry-handler`: solo 429/503/timeout/conectividad; los 4xx van a `APP:NON_RETRYABLE` |
| Reintento nivel 3 | NACK → reentrega de Anypoint MQ → DLQ nativa (`maxDeliveries = 5`) |
| Reintento nivel 4 | `POST /dlq/replay` en `cdc-control-api` |
| Transaccionalidad | tRFC (`sap-idoc-cdc`, la excepción regresa a SAP), transacción local por fila (`hana-trigger-cdc`), token tras la última página (`sap-odp-cdc`) |
| Trazabilidad | `correlationId` en el sobre y en las propiedades MQ; logs JSON por etapa (`stageEvent`); `cdc.audit` → `CDC_AUDIT` |

## Pruebas (MUnit 3.7.5)

| Suite | Casos |
|---|---|
| `adls-delivery-test-suite.xml` | Camino feliz (registro de cdcId + ACK tras commit) · 503 persistente → 6 intentos + NACK, sin registrar cdcId · 400 → DLQ sin reintento · duplicado ya comprometido → ACK sin reprocesar · reentrega no comprometida → se reprocesa · sobre inválido → DLQ |
| `sap-odp-cdc-test-suite.xml` | Última página → publica y avanza el token · 4xx → token no avanza · 503 → reintentos y token no avanza |
| `*-smoke-test-suite.xml` (6) | Invoca cada flujo con conectores mockeados para forzar la compilación de todo el DataWeave (MUnit inicializa de forma *lazy*) |
| `sap-idoc-cdc` / `sap-initial-load` | Sin JCo los flujos SAP no inicializan; se prueban sus DataWeave: IDoc DEBMAS07 → KNA1/KNVV y plan de 68 particiones + página RFC → `op = R` |

Resultado verificado: **6 proyectos · 17 pruebas · 0 fallas · `BUILD SUCCESS`** (cobertura `adls-delivery` 94 %, `sap-odp-cdc` 95 %).
Bugs que encontraron las pruebas y ya están corregidos: palabra reservada `type` en DataWeave, precedencia de `not … contains`,
campos `__next`/`__delta` sin comillas, `protocol="HTTPS"` obligatorio en Mule 4.11 y URL de token con marcador inválido.

> **Windows:** el runtime embebido de MUnit no tolera rutas con acentos (`Artes Gráficas` → `Gr?ficas`).
> Ejecutar `mvn clean package` copiando el proyecto a una ruta ASCII (p. ej. `C:\work\cdc\`) o desde CI.

## Construir

```bash
export JAVA_HOME=<JDK 17>
cd adls-delivery && mvn clean package          # compila + MUnit
```

## Configuración por entorno

* `src/main/resources/config-<env>.yaml` — valores no sensibles (colas, URLs, frecuencias).
* `src/main/resources/secure-<env>.yaml` — secretos cifrados con el *Secure Properties Tool* (AES/CBC):
  `![valorCifrado]`. Llave en runtime: `-Dmule.key=...` (Secrets Manager / propiedad protegida en CloudHub 2.0).
* Entorno activo: propiedad `env` (por defecto `dev`).

## Prerrequisitos fuera de Mule

| Tema | Acción |
|---|---|
| **SAP JCo** | `sap-idoc-cdc` y `sap-initial-load` requieren `sapjco3.jar` + `libsapjco3.so` (Linux, CloudHub 2.0) del SAP Service Marketplace. Ver el bloque comentado en sus `pom.xml`. |
| SAP Gateway / ODP | Servicios OData `ZODP_*_SRV` (SEGW / extractores genéricos RSO2), listados en `sap-odp-cdc/src/main/resources/subscriptions.json`. |
| Change pointers | BD50/BD52/BD61 para DEBMAS, CREMAS, MATMAS; puerto RFC (WE21) + destino TCP/IP (SM59) con Program ID `MULE_CDC_MAESTROS`. |
| `Z_CDC_READ_TABLE` | Módulo de función RFC (lectura paginada: `IV_TABLE`, `IV_WHERE`, `IV_OFFSET`, `IV_ROWS` → `ET_DATA` JSON). Evita el límite de 512 caracteres de `RFC_READ_TABLE`. |
| HANA (solo Opción 1) | `hana-trigger-cdc/sql/zcdc_log.sql`. Requiere **licencia HANA enterprise**. |
| Azure SQL | `cdc-control-api/sql/cdc_audit.sql`. |
| Anypoint MQ | Colas `cdc.q.maestros`, `cdc.q.ventas`, `cdc.q.finanzas`, `cdc.q.catalogos`, `cdc.audit`, `cdc.cmd.resync`; DLQ `cdc.dlq` asignada a cada cola con `maxDeliveries = 5`. |
| ADLS Gen2 | Namespace jerárquico habilitado (rename atómico); Service Principal con *Storage Blob Data Contributor*. |

## Notas de la generación

* Proyectos creados con `anypoint-cli-v4 dx mule project create` y versiones de conectores resueltas desde Exchange
  (skill `build-mule-integration`).
* Driver HANA: `com.sap.cloud.db.jdbc:ngdbc:2.30.11` (la versión 2.22.8 de la referencia no existe en Maven Central).
* Falsos positivos conocidos de `validate_before_build.sh` en Windows: prefijos `os` / `secure-properties` / `batch`
  y XSD de APIkit; además `jq` emite CRLF (se usó un *shim* que elimina `\r`). Maven y MUnit validan contra los XSD reales.

## Revisión independiente (aplicada)

Una revisión independiente del código encontró, y ya se corrigieron:

1. **Pérdida silenciosa:** el `cdcId` se marcaba como procesado al recibirlo. Ahora se registra solo tras el commit.
2. **Saltos en HANA:** el watermark por `SEQ` podía omitir filas confirmadas tarde. Se cambió a lectura por `STATUS = 'NEW'`.
3. **Replay de la DLQ:** fallaba con mensajes que Anypoint MQ movió directo a la DLQ (sin sobre de error).
4. **`cdcId` aleatorio:** ahora es determinista.
5. **`/resync` concurrente:** ACK inmediato y `maxConcurrency=1`.
6. **Paquetes de IDocs:** solo se leía el primero; ahora se procesan todos.
7. **Paginación recursiva:** ahora es un bucle acotado.
8. **Commit por tabla:** antes se hacía por lote.
9. **Auditoría:** ahora es *best-effort* y no revierte la captura.
10. **Conciliación diaria:** se agregó el flujo programado (`reconcile-daily-flow`, 03:00).

### Pendiente operativo (no es código)

* **Inicialización ODP:** la primera llamada con `odata.track-changes` es una *delta init* **con datos**. En producción hay que inicializar las
  suscripciones **sin datos** (Basis / ODQMON) y dejar el histórico a `sap-initial-load`. Si no, la primera corrida de 5 o 15 minutos traería
  tablas completas, por ejemplo GLPCA con 20 GB.
* **`cdc.q.catalogos`:** no tiene productor en este código de referencia (`sap-rfc-snapshot` no se incluyó).
