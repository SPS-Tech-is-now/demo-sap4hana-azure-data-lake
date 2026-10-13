/**
 * Fila de ZCDC_LOG (+ imagen after leída de la tabla origen) → sobre CDC.
 * vars.logRow  : { SEQ, TABNAME, OP, KEY_JSON, CHANGE_TS }
 * vars.key     : llave parseada
 * payload      : resultado del SELECT sobre la tabla origen (vacío si op = D o si ya no existe)
 */
%dw 2.0
import * from dw::cdc::Envelope
output application/java

var row   = vars.logRow
var after = payload[0] default null
// Si el registro ya no existe al leerlo (borrado posterior), se emite como D.
var op    = if (row.OP != "D" and after == null) "D" else row.OP
---
envelope(
    { system: Mule::p("sap.systemId"), client: Mule::p("sap.client"), mechanism: "TRIGGER", table: row.TABNAME },
    op,
    vars.key,
    row.SEQ as Number,
    "ZCDC_LOG:" ++ (row.SEQ as String),
    if (op == "D") null else after,
    if (op == "D") vars.key else null,
    null,
    (row.CHANGE_TS as LocalDateTime) as String { format: "yyyy-MM-dd'T'HH:mm:ss" } ++ "Z",
    "ZCDC_LOG:" ++ (row.SEQ as String)
)
