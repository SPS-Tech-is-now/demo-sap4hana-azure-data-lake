/**
 * Agrupa el micro-lote por tabla y arma un archivo JSON Lines por tabla.
 * Entrada : [{ record: <sobre CDC>, ackToken, correlationId }]
 * Salida  : [{ table, batchId, count, minSeq, maxSeq, content, bytes, sha256,
 *              tmpPath, finalPath, manifestPath, records, cdcIds, ackTokens, correlationIds }]
 * Cada tabla es una unidad de commit independiente con sus propios ackTokens.
 */
%dw 2.0
import hashWith from dw::Crypto
import toHex from dw::core::Binaries
output application/java

var batchId  = uuid()
var ts       = now() >> "UTC"
var root     = Mule::p("adls.rootPath")
var system   = lower(Mule::p("sap.systemId"))
var dayPath  = "yyyy=" ++ (ts as String {format: "yyyy"}) ++ "/mm=" ++ (ts as String {format: "MM"}) ++ "/dd=" ++ (ts as String {format: "dd"})

fun safe(t: String): String = lower(t replace "/" with "_")
---
(payload groupBy ((item) -> item.record.source.table)) pluck ((items, table) -> do {
    var records  = items.record
    var tbl      = safe(table as String)
    var fileName = batchId ++ ".jsonl"
    var content  = (records map ((r) -> write(r, "application/json", {indent: false}))) joinBy "\n"
    var bin      = content as Binary {encoding: "UTF-8"}
    ---
    {
        table:          table as String,
        batchId:        batchId,
        count:          sizeOf(records),
        minSeq:         min(records.seq),
        maxSeq:         max(records.seq),
        content:        content,
        bytes:          sizeOf(bin),
        sha256:         lower(toHex(hashWith(bin, "SHA-256"))),
        tmpPath:        "_tmp/" ++ tbl ++ "-" ++ fileName,
        finalPath:      root ++ "/" ++ system ++ "/" ++ tbl ++ "/" ++ dayPath ++ "/" ++ fileName,
        manifestPath:   "_manifest/" ++ tbl ++ "/" ++ dayPath ++ "/" ++ batchId ++ ".json",
        records:        records,
        cdcIds:         records.cdcId,
        ackTokens:      items.ackToken,
        correlationIds: (items.correlationId distinctBy $)
    }
})
