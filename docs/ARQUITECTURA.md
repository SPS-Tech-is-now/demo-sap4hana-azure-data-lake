# Arquitectura: CDC de SAP a Azure Data Lake con MuleSoft

> Documento técnico de referencia para **Artes Gráficas**. Para la visión ejecutiva, ver el [README](../README.md).
> Para la presentación interactiva, ver [`html/ArtesGraficas_CDC_MuleSoft.html`](../html/ArtesGraficas_CDC_MuleSoft.html).

**Contenido**

1. [Vista general](#1-vista-general)
2. [Capas API-led](#2-capas-api-led)
3. [El sobre CDC: un solo contrato](#3-el-sobre-cdc-un-solo-contrato)
4. [Captura por dominio](#4-captura-por-dominio)
5. [Entrega garantizada de punta a punta](#5-entrega-garantizada-de-punta-a-punta)
6. [Reintentos en 4 niveles](#6-reintentos-en-4-niveles)
7. [Commit atómico en ADLS Gen2](#7-commit-atómico-en-adls-gen2)
8. [Ciclo de vida de un cambio](#8-ciclo-de-vida-de-un-cambio)
9. [Trazabilidad y conciliación](#9-trazabilidad-y-conciliación)
10. [Carga inicial y paso a delta](#10-carga-inicial-y-paso-a-delta)
11. [Despliegue en CloudHub 2.0](#11-despliegue-en-cloudhub-20)

---

## 1. Vista general

```mermaid
flowchart LR
    subgraph SAP["🏭 SAP ECC on HANA"]
        CP["Change pointers<br/>IDoc DEBMAS · CREMAS · MATMAS"]
        ODP["ODP / ODQ<br/>delta vía OData"]
        LOG["ZCDC_LOG<br/>triggers HANA *"]
        TAB["Tablas y catálogos<br/>RFC"]
    end

    subgraph MULE["⚙️ MuleSoft · CloudHub 2.0 Private Space"]
        direction LR
        subgraph CAP["Captura · System APIs"]
            IDOC["sap-idoc-cdc"]
            ODPC["sap-odp-cdc"]
            TRG["hana-trigger-cdc"]
            INI["sap-initial-load"]
        end
        subgraph MQ["Anypoint MQ"]
            QM[["cdc.q.maestros"]]
            QV[["cdc.q.ventas"]]
            QF[["cdc.q.finanzas"]]
            DLQ[["cdc.dlq"]]
            AUD[["cdc.audit"]]
        end
        DEL["adls-delivery<br/>Process API"]
        CTL["cdc-control-api<br/>Experience API"]
    end

    subgraph AZ["☁️ Microsoft Azure"]
        ADLS[("ADLS Gen2<br/>bronze/ · _manifest/")]
        SQL[("Azure SQL<br/>CDC_AUDIT")]
        CONS["Databricks · Fabric · Synapse"]
    end

    CP -- tRFC --> IDOC
    ODP -- OData --> ODPC
    LOG -- JDBC --> TRG
    TAB -- RFC --> INI

    IDOC --> QM
    ODPC --> QV & QF
    TRG --> QV & QF
    INI --> QM & QV & QF

    QM & QV & QF --> DEL
    DEL -- "commit atómico" --> ADLS
    DEL -. "4xx / reintentos agotados" .-> DLQ
    DEL & IDOC & ODPC & TRG --> AUD
    AUD --> CTL --> SQL
    CTL -. "replay" .-> DLQ
    ADLS --> CONS

    classDef cap fill:#E0218A,color:#fff,stroke:#b0186c
    classDef bus fill:#00A3E0,color:#fff,stroke:#0077b6
    classDef proc fill:#0B2545,color:#fff,stroke:#071833
    classDef bad fill:#D7263D,color:#fff,stroke:#a0182b
    class IDOC,ODPC,TRG,INI cap
    class QM,QV,QF,AUD bus
    class DEL,CTL proc
    class DLQ bad
```

<sub>* `hana-trigger-cdc` solo aplica con licencia HANA *enterprise*. Con licencia *runtime*, ODP, IDoc y RFC cubren todo.</sub>

**Principio rector:** *nada se confirma antes de que el siguiente paso lo haya persistido.* Esto aplica al ACK de MQ, al delta token de ODP, a la marca `PROCESSED` en HANA y al tRFC del IDoc.

---

## 2. Capas API-led

```mermaid
flowchart TB
    subgraph EXP["Experience"]
        CTL["cdc-control-api<br/>/trace · /health/lag · /reconcile · /resync · /dlq/replay"]
    end
    subgraph PRO["Process"]
        DEL["adls-delivery<br/>idempotencia · micro-lotes · commit atómico · DLQ"]
    end
    subgraph SYS["System"]
        A["sap-idoc-cdc"]
        B["sap-odp-cdc"]
        C["hana-trigger-cdc"]
        D["sap-initial-load"]
    end
    SYS == "sobre CDC común<br/>(Anypoint MQ)" ==> PRO
    PRO == "eventos de auditoría" ==> EXP
    EXP -. "comandos resync / replay" .-> SYS
    EXP -. "replay DLQ" .-> PRO
```

| Proyecto | Responsabilidad | Disparador |
|---|---|---|
| `sap-idoc-cdc` | Datos maestros por IDoc con tRFC | `sap:document-listener` |
| `sap-odp-cdc` | Deltas ODP; el token avanza solo después de publicar todo | `scheduler` cada 5 o 15 min |
| `hana-trigger-cdc` | Log de triggers, con una transacción por fila | `scheduler` cada 1 min |
| `sap-initial-load` | Carga histórica con Batch Job y recargas `/resync` | cron nocturno + MQ |
| `adls-delivery` | ACK manual, idempotencia, commit atómico, DLQ | `anypoint-mq:subscriber` |
| `cdc-control-api` | Operación, auditoría y conciliación diaria | `http:listener` + cron 03:00 |

---

## 3. El sobre CDC: un solo contrato

Todas las fuentes publican **el mismo sobre**. Por eso los mecanismos de captura son intercambiables: la entrega no sabe de dónde viene el dato. Esquema: [`mule/cdc-common/cdc-envelope.schema.json`](../mule/cdc-common/cdc-envelope.schema.json).

```mermaid
classDiagram
    class SobreCDC {
        +String cdcId
        +String correlationId
        +Source source
        +String op
        +Object key
        +String changeTs
        +Number seq
        +String watermark
        +Object before
        +Object after
    }
    class Source {
        +String system
        +String client
        +String mechanism
        +String table
    }
    SobreCDC *-- Source
```

| Campo | Significado |
|---|---|
| `cdcId` | Hash determinista de la posición en SAP; llave de idempotencia |
| `correlationId` | Hilo de trazabilidad (DOCNUM, request ODQ, SEQ) |
| `op` | `I` alta · `U` cambio · `D` borrado · `R` foto (carga inicial) |
| `key` | Llave de negocio SAP para el MERGE |
| `seq` | Orden: en el MERGE gana el más alto |
| `watermark` | Posición en la fuente |
| `source.mechanism` | `IDOC` · `ODP` · `TRIGGER` · `SNAPSHOT` |

**El `cdcId` es determinista.** Se calcula como `SHA1(sistema, tabla, op, llave, semilla)`, donde la semilla es la posición en el origen:

| Mecanismo | Semilla |
|---|---|
| IDoc | `DOCNUM | segmento` |
| ODP | `delta link | página | fila` |
| Trigger | `ZCDC_LOG:SEQ` |
| Carga inicial | `partición | corrida | offset` |

Si SAP reenvía un IDoc o se repite un delta, el id es **el mismo** y el lago no se duplica.

---

## 4. Captura por dominio

```mermaid
flowchart LR
    M["🧾 Maestros<br/>KNA1 · LFA1 · MARA · MAKT…"] -->|"IDoc · por evento · menos de 5 min"| Q1[["cdc.q.maestros"]]
    V["🛒 Ventas, compras y producción<br/>VBAK · VBAP · EKKO · AFKO…"] -->|"ODP delta · cada 5 min"| Q2[["cdc.q.ventas"]]
    F["💰 Finanzas, costos e inventario<br/>GLPCA · BSIS · BKPF · COSP · MSEG…"] -->|"ODP / trigger · cada 15 min"| Q3[["cdc.q.finanzas"]]
    C["📚 Catálogos<br/>T001 · TCURR · TVM*T…"] -->|"RFC snapshot · diario 02:00"| Q4[["cdc.q.catalogos"]]
```

Volumetría real (2026): **~15,000 cambios por día hábil**, unos 25 por minuto en pico, y alrededor de 75,000 por día en el cierre de mes (×5). La operación diaria es ligera; el único desafío de volumen es la **carga inicial de ~75 GB**.

---

## 5. Entrega garantizada de punta a punta

```mermaid
sequenceDiagram
    autonumber
    participant SAP as SAP ECC
    participant CAP as Captura (System)
    participant MQ as Anypoint MQ
    participant DEL as adls-delivery
    participant OS as Object Store
    participant ADLS as ADLS Gen2

    SAP->>CAP: cambio (IDoc / delta ODP / fila ZCDC_LOG)
    CAP->>CAP: sobre CDC con cdcId determinista
    CAP->>MQ: publish (cola del dominio)
    CAP-->>SAP: confirma: tRFC OK · avanza token · marca PROCESSED
    Note over CAP,SAP: Si publish falla, SAP NO recibe confirmación y reintenta

    MQ->>DEL: entrega (ACK MANUAL, el mensaje sigue en vuelo)
    DEL->>OS: os:contains(cdcId)?
    alt ya comprometido
        DEL->>MQ: ACK (duplicado, no se reescribe)
    else nuevo o no comprometido
        DEL->>DEL: micro-lote (60 s o 5,000)
        DEL->>ADLS: _tmp → rename → _manifest
        DEL->>OS: os:store(cdcId) solo tras el manifiesto
        DEL->>MQ: ACK
    end
```

> **Garantía:** *al menos una vez* + **idempotencia por cdcId** = **efectivamente una vez**.
> Anypoint MQ no participa en transacciones XA; la transaccionalidad de punta a punta se logra con **ACK manual, commit en orden e idempotencia**.

---

## 6. Reintentos en 4 niveles

```mermaid
flowchart TD
    E(["Error al escribir en ADLS"]) --> T{"¿Tipo de error?"}
    T -->|"429 · 503 · timeout · conectividad"| L2["Nivel 2 · until-successful<br/>5 reintentos selectivos"]
    T -->|"400 · 4xx · contenido inválido"| DLQ[["cdc.dlq<br/>con sobre de error"]]
    L2 -->|"se recupera"| OK(["✅ Commit + ACK"])
    L2 -->|"agotado"| L3["Nivel 3 · NACK<br/>Anypoint MQ reentrega"]
    L3 -->|"Azure vuelve"| OK
    L3 -->|"5 entregas fallidas"| DLQ
    DLQ --> L4["Nivel 4 · POST /dlq/replay<br/>tras corregir la causa"]
    L4 --> OK
    L1["Nivel 1 · reconexión de conectores<br/>SAP · MQ · ADLS"] -.-> T

    style OK fill:#12A150,color:#fff
    style DLQ fill:#D7263D,color:#fff
    style L1 fill:#0B2545,color:#fff
    style L2 fill:#E89B0C,color:#fff
    style L3 fill:#00A3E0,color:#fff
    style L4 fill:#E0218A,color:#fff
```

---

## 7. Commit atómico en ADLS Gen2

```mermaid
sequenceDiagram
    autonumber
    participant DEL as adls-delivery
    participant TMP as _tmp/
    participant BRZ as bronze/sap/ecc_prd/tabla/yyyy=/mm=/dd=/
    participant MAN as _manifest/
    participant MQ as Anypoint MQ

    DEL->>TMP: create + append + flush (batchId.jsonl)
    Note right of TMP: invisible para consumidores
    DEL->>BRZ: rename atómico (namespace jerárquico)
    DEL->>MAN: manifiesto {count, minSeq, maxSeq, sha256, correlationIds}
    Note right of MAN: commit lógico: solo se procesa lo que tiene manifiesto
    DEL->>MQ: ACK de los mensajes de ESA tabla
```

Cada tabla del micro-lote es una **unidad de commit independiente**: si una falla, las demás se confirman normalmente.

Ejemplo de consumo con Databricks:

```sql
MERGE INTO silver.vbap t USING bronze_vbap s
  ON t.VBELN = s.key.VBELN AND t.POSNR = s.key.POSNR
WHEN MATCHED AND s.seq > t.seq AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.seq > t.seq THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT *
```

---

## 8. Ciclo de vida de un cambio

```mermaid
stateDiagram-v2
    [*] --> CAPTURED: cambio en SAP
    CAPTURED --> PUBLISHED: sobre en Anypoint MQ
    PUBLISHED --> RECEIVED: adls-delivery lo toma
    RECEIVED --> DUPLICATE: cdcId ya comprometido
    DUPLICATE --> [*]: ACK sin reescribir
    RECEIVED --> LANDED: _tmp → rename
    LANDED --> COMMITTED: manifiesto + cdcId registrado + ACK
    COMMITTED --> [*]
    LANDED --> RETRY: 429 / 503
    RETRY --> LANDED: se recupera
    RETRY --> PUBLISHED: NACK → reentrega
    RECEIVED --> DLQ: sobre inválido / 4xx
    DLQ --> PUBLISHED: /dlq/replay
```

---

## 9. Trazabilidad y conciliación

```mermaid
flowchart LR
    subgraph Eventos
        P["PUBLISHED<br/>(captura)"]
        C["COMMITTED<br/>(entrega)"]
    end
    P & C --> AUD[["cdc.audit"]] --> API["cdc-control-api"] --> SQL[("CDC_AUDIT")]
    SQL --> TR["GET /trace/{correlationId}<br/>recorrido de un cambio"]
    SQL --> LAG["GET /health/lag<br/>rezago vs SLA"]
    CRON(["⏰ 03:00 diario"]) --> REC["Conciliar día anterior<br/>capturado vs. manifiestos ADLS"]
    SQL --> REC
    REC -->|MATCH| OK(["✅"])
    REC -->|MISMATCH| RS["/resync de la tabla<br/>+ alerta"]
```

- **Un correlationId de punta a punta:** DOCNUM del IDoc, request de ODQ o SEQ del log.
- **Logs JSON por etapa:** `CAPTURED → PUBLISHED → RECEIVED → LANDED → COMMITTED` (o `DUPLICATE`, `RETRY`, `NACK`, `DLQ`).
- **Anypoint Monitoring:** alertas si la DLQ tiene más de 0 mensajes, si el rezago supera el SLA o si la conciliación da `MISMATCH`.

---

## 10. Carga inicial y paso a delta

```mermaid
sequenceDiagram
    participant OPS as Operación
    participant CDC as Captura delta
    participant MQ as Anypoint MQ
    participant INI as sap-initial-load
    participant LAKE as ADLS

    OPS->>CDC: 1 · activar CDC y fijar T0
    CDC->>MQ: los deltas desde T0 se acumulan
    OPS->>INI: 2 · ventana nocturna
    loop por partición tabla × BUKRS × GJAHR (máx. 4 en paralelo)
        INI->>MQ: filas op=R, seq=0
    end
    MQ->>LAKE: 3 · se libera el consumo
    Note over LAKE: MERGE: cualquier delta (seq > 0) gana sobre la foto inicial
    OPS->>LAKE: 4 · conciliación
```

**Reanudable:** el estado de cada partición (`PENDING`, `RUNNING`, `DONE` o `FAILED`) vive en Object Store. Si la app se detiene, la siguiente ventana continúa donde se quedó.

---

## 11. Despliegue en CloudHub 2.0

```mermaid
flowchart LR
    subgraph ONP["Red Artes Gráficas"]
        ECC["SAP ECC on HANA<br/>Gateway RFC · SAP Gateway OData"]
    end
    subgraph PS["CloudHub 2.0 · Private Space"]
        APPS["6 apps Mule<br/>~1.1 vCores permanentes"]
        OS2[("Object Store v2")]
        MON["Anypoint Monitoring"]
    end
    subgraph AZR["Azure"]
        PE["Private Endpoint"] --> ADLS[("ADLS Gen2 · HNS")]
        SQL[("Azure SQL")]
        ENTRA["Entra ID<br/>Service Principal"]
    end
    ECC <-->|"VPN IPsec"| PS
    APPS --> OS2
    APPS --> MON
    APPS -->|"OAuth 2.0 client credentials"| ENTRA
    APPS --> PE
    APPS --> SQL
```

| Aplicación | Tamaño |
|---|---|
| `adls-delivery` | 0.2 vCore × 2 réplicas |
| `sap-idoc-cdc`, `sap-odp-cdc` | 0.2 vCore c/u |
| `hana-trigger-cdc`, `cdc-control-api` | 0.1 vCore c/u |
| `sap-initial-load` | 1 vCore **temporal** (se apaga al terminar) |

Secretos cifrados con Secure Properties (AES/CBC), TLS 1.2+, API Manager (Client ID Enforcement + OAuth) en la API de control y RBAC *Storage Blob Data Contributor* acotado al contenedor.
