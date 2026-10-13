/**
 * Evento de cdc.audit → filas CDC_AUDIT.
 *  - Fila resumen (CORRELATION_ID = NULL, RECORDS = conteo) para conciliación y rezago.
 *  - Una fila por correlationId (RECORDS = NULL) para /trace.
 * Eventos esperados:
 *  COMMITTED (adls-delivery): { files: [{table, path, count, correlationIds}] }
 *  PUBLISHED (captura)      : { table, records, correlationIds? }
 */
%dw 2.0
output application/java
var e = if (payload is String) read(payload, "application/json") else payload
var ts = (e.ts default now()) as DateTime
fun row(table, corr, records, detail) = {
    eventTs: ts as LocalDateTime,
    app: e.app default "unknown",
    stage: e.stage default "UNKNOWN",
    tabname: table,
    correlationId: corr,
    records: records,
    detail: detail
}
fun rowsFor(table, count, corrs, detail) =
    [row(table, null, count, detail)] ++ ((corrs default []) map ((c) -> row(table, c, null, detail)))
---
if (e.stage == "COMMITTED")
    flatten((e.files default []) map ((f) -> rowsFor(f.table, f.count, f.correlationIds, f.path)))
else
    rowsFor(e.table, e.records, e.correlationIds, write(e -- ["ts", "app", "stage"], "application/json", {indent: false}))
