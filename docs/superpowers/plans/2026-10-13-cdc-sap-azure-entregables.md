# CDC SAP → Azure (Artes Gráficas) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Producir el HTML de presentación (marca SPS, cliente Artes Gráficas) y los proyectos Mule 4 de referencia que materializan el planteamiento CDC.

**Architecture:** HTML single-file con SVG/JS inline basado en el design system SPS (`report-styles.css`). Código Mule en `mule/` con un proyecto Maven por app, compartiendo el sobre CDC definido en `cdc-common`.

**Tech Stack:** HTML5/CSS/JS vanilla; Mule 4.6+, Java 17, Anypoint MQ, Object Store v2, SAP Connector 5.x, Database Connector, HTTP Connector, Aggregators module, Batch, MUnit 3.

**Spec:** `docs/superpowers/specs/2026-10-13-cdc-sap-azure-entregables-design.md` + `Planteamiento_CDC_SAP_Azure_MuleSoft.md`

## Global Constraints
- Idioma: español en HTML, README y comentarios; identificadores Mule en inglés/kebab-case.
- Cifras SOLO del planteamiento: 63 tablas, ~75 GB, ~3.0 M cambios 2026, ~10,500/día natural, ~15,000/día hábil, ~25/min pico, ~1.1 vCores, top 5 (GLPCA ~4,150 · BSIS ~2,950 · MSEG ~1,570 · COSP ~1,360 · BKPF ~1,240).
- Nombres de apps: `sap-idoc-cdc`, `sap-odp-cdc`, `hana-trigger-cdc`, `sap-rfc-snapshot`, `adls-delivery`, `cdc-control-api`, `sap-initial-load`.
- Exchange `cdc.events`; colas `cdc.q.maestros`, `cdc.q.ventas`, `cdc.q.finanzas`, `cdc.q.catalogos`, DLQ `cdc.dlq`, auditoría `cdc.audit`.
- Sobre CDC: campos `cdcId, correlationId, source{system,client,mechanism,table}, op(I|U|D|R), key, changeTs, seq, watermark, before, after`.
- Endpoints control: `GET /trace/{correlationId}`, `GET /health/lag`, `POST /reconcile`, `POST /resync`, `POST /dlq/{queue}/replay`.
- Logo SPS embebido base64 desde `C:\rolando\SPS\2019\Nueva Imagen\SPS_Nueva Imagen\SPS_Nueva Imagen\Logos\SPS.png`; footer "Confidencial · Artes Gráficas × SPS · 2026" + www.spsolutions.com.mx.
- Secretos solo vía `${secure::...}`; nunca credenciales en claro.
- No hay git: los pasos "commit" se omiten.

## Review Focus
1. Duplicado del mismo `cdcId` llega dos veces → adls-delivery lo descarta sin escribir y hace ACK (test MUnit en Task 4).
2. ADLS responde 503 repetido → until-successful agota, NACK, mensaje NO se pierde (test MUnit Task 4).
3. Error no reintentable (400 / transformación) → directo a DLQ con sobre de error, ACK del original (test MUnit Task 4).
4. ODP falla a mitad de paginación → delta token NO se avanza (assert en MUnit Task 3).
5. HTML en móvil (≤ 420 px) y modo oscuro → sin scroll horizontal, simulador usable (check Task 7).

---

### Task 1: cdc-common (contrato)
**Files:** Create `mule/cdc-common/cdc-envelope.schema.json`, `mule/cdc-common/dw/cdc-envelope.dwl`, `mule/cdc-common/dw/error-envelope.dwl`, `mule/cdc-common/samples/vbap-update.json`, `mule/cdc-common/README.md`
**Produces:** función DW `envelope(src: Object, mechanism: String, table: String, op: String, key: Object, seq: Number, watermark: String, before, after)` y `errorEnvelope(err, payload, attempts)`.
- [ ] Escribir JSON Schema (draft 2020-12) con `op` enum y required fields.
- [ ] Escribir sample y validar: `python -I -c "import json,jsonschema;jsonschema.validate(json.load(open(s)),json.load(open(sc)))"` → sin excepción (si jsonschema no está, validación manual de campos con script).
- [ ] Escribir los .dwl (copiados a `src/main/resources/dw/` de cada app que los use).

### Task 2: sap-idoc-cdc
**Files:** `mule/sap-idoc-cdc/{pom.xml, mule-artifact.json, src/main/mule/sap-idoc-cdc.xml, src/main/mule/global.xml, src/main/resources/config-dev.yaml, src/main/resources/dw/debmas-to-cdc.dwl, src/main/resources/dw/matmas-to-cdc.dwl}`
- [ ] `sap:document-listener` (gatewayHost/programId, `transactionalAction=ALWAYS_BEGIN`), choice por IDOCTP (DEBMAS→KNA1/KNVV, MATMAS→MARA/MAKT/MARM/MVKE), publish `anypoint-mq:publish` a `cdc.events` con properties `correlationId`=DOCNUM, `table`, `op`.
- [ ] Error handler `on-error-propagate` (excepción vuelve a SAP → SM58) + log JSON etapa `CAPTURED`/`PUBLISHED`.
- [ ] Verificar XML bien formado: `python -I -c "import xml.dom.minidom,sys;[xml.dom.minidom.parse(f) for f in sys.argv[1:]]" <archivos>`.

### Task 3: sap-odp-cdc
**Files:** `mule/sap-odp-cdc/{pom.xml, mule-artifact.json, src/main/mule/sap-odp-cdc.xml, global.xml, src/main/resources/config-dev.yaml, subscriptions.yaml, dw/odp-to-cdc.dwl}`, `src/test/munit/odp-token-test.xml`
- [ ] Scheduler por dominio (ventas 5 min, finanzas 15 min) → foreach suscripción → `os:retrieve` delta link → loop de páginas (`__next`) con `Prefer: odata.track-changes, odata.maxpagesize=1000` → split + publish MQ.
- [ ] `os:store` del nuevo `__delta` SOLO tras última página; try con on-error-propagate que no toca el store.
- [ ] MUnit: mock 2ª página con error → verify `os:store` llamado 0 veces.
- [ ] Verificar XML bien formado.

### Task 4: adls-delivery (núcleo de garantías)
**Files:** `mule/adls-delivery/{pom.xml, mule-artifact.json, src/main/mule/adls-delivery.xml, adls-client.xml, global.xml, src/main/resources/config-dev.yaml, dw/batch-jsonl.dwl, dw/manifest.dwl, dw/error-envelope.dwl}`, `src/test/munit/adls-delivery-test.xml`
- [ ] Subscriber `acknowledgementMode=MANUAL`, `idempotent-message-validator` (idExpression `payload.cdcId`, OS v2 TTL 7d) → duplicado: ACK y fin.
- [ ] `aggregators:size-based-aggregator` (5000) + `time-based` (60 s) acumulando `{payload, ackToken}`; listener del grupo → write.
- [ ] Sub-flujo `adls-commit`: create/append/flush a `_tmp/<batchId>.jsonl` → rename (`x-ms-rename-source`) → manifest → `anypoint-mq:ack` por token. OAuth2 client-credentials a Entra ID.
- [ ] `until-successful maxRetries=5 millisBetweenRetries=2000` envolviendo solo llamadas HTTP; error handler: tipos reintentables → `anypoint-mq:nack`; resto → publish `cdc.dlq` con errorEnvelope + ACK.
- [ ] MUnit 3 tests (Review Focus 1–3): duplicado→ack sin http; 503 persistente→nack; 400→publish dlq + ack.
- [ ] Verificar XML bien formado.

### Task 5: hana-trigger-cdc
**Files:** `mule/hana-trigger-cdc/{pom.xml, mule-artifact.json, src/main/mule/hana-trigger-cdc.xml, global.xml, config-dev.yaml, dw/log-to-cdc.dwl}`, `mule/hana-trigger-cdc/sql/zcdc_log.sql`
- [ ] DDL `ZCDC_LOG(SEQ BIGINT IDENTITY, TABNAME, OP, KEY_JSON NCLOB, ROW_JSON NCLOB, CHANGE_TS, STATUS)` + trigger AFTER INSERT/UPDATE/DELETE sobre VBAP y BSID (ejemplo D por compensación).
- [ ] Flujo: scheduler 15 min → `db:select` `STATUS='NEW' ORDER BY SEQ LIMIT 2000` → publish MQ → `db:update STATUS='PROCESSED' WHERE SEQ BETWEEN :min AND :max`, todo en `try transactionalAction=ALWAYS_BEGIN`.
- [ ] Verificar XML bien formado.

### Task 6: cdc-control-api + sap-initial-load + README
**Files:** `mule/cdc-control-api/{pom.xml, mule-artifact.json, src/main/resources/api/cdc-control-api.raml, src/main/mule/cdc-control-api.xml, global.xml, config-dev.yaml}`, `mule/sap-initial-load/{pom.xml, mule-artifact.json, src/main/mule/sap-initial-load.xml, global.xml, config-dev.yaml, partitions.yaml}`, `mule/README.md`
- [ ] RAML 1.0 con los 5 endpoints; flujos APIkit: trace (db select CDC_AUDIT), lag, reconcile (RFC count vs suma manifests), resync (publica comando), dlq replay (consume N de cdc.dlq → republish → ack).
- [ ] Batch Job `maxConcurrency=4`, blockSize 500; input = particiones con estado ≠ DONE desde OS; step lee RFC por partición → sobre `op=R` → publish; onComplete actualiza estado + log reporte.
- [ ] README: mapa de proyectos, cómo correr, sap-rfc-snapshot descrito, limitación "no compilado contra Exchange".
- [ ] Verificar RAML/XML/YAML parsean.

### Task 7: HTML de presentación
**Files:** `html/ArtesGraficas_CDC_MuleSoft.html` (script de build opcional en scratchpad para embeber logo base64)
- [ ] Base `report-styles.css` de la skill enterprise-proposal-html, variables `--brand-*` adaptadas (azul tinta/cian, evocando artes gráficas: CMYK accent).
- [ ] Secciones 1–9 del spec; arquitectura SVG animada; simulador de 4 escenarios con log de eventos; commit atómico paso a paso; timeline de trazabilidad; roadmap.
- [ ] Checks: script python verifica `data:image/png;base64` presente, "Artes Gráficas" y "spsolutions.com.mx" presentes, cifras globales presentes; render en Chrome (si disponible) en 1280 px y 400 px, modo oscuro.
