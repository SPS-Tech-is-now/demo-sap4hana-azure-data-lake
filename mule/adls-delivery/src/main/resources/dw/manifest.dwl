/**
 * Manifiesto del archivo comprometido. Regla para consumidores:
 * solo se procesan archivos con manifiesto (commit lógico).
 */
%dw 2.0
output application/java
var manifest = {
    batchId:        vars.file.batchId,
    table:          vars.file.table,
    dataPath:       vars.file.finalPath,
    format:         "jsonl",
    count:          vars.file.count,
    minSeq:         vars.file.minSeq,
    maxSeq:         vars.file.maxSeq,
    sha256:         vars.file.sha256,
    bytes:          vars.file.bytes,
    correlationIds: vars.file.correlationIds,
    committedAt:    (now() >> "UTC") as String {format: "yyyy-MM-dd'T'HH:mm:ss'Z'"},
    producer:       Mule::p("app.name") default "adls-delivery"
}
var content = write(manifest, "application/json")
---
{
    content: content,
    bytes:   sizeOf(content as Binary {encoding: "UTF-8"})
}
