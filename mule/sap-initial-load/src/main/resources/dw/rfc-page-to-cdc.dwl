/**
 * Respuesta de Z_CDC_READ_TABLE (ET_DATA = JSON array de filas) → sobres CDC op = R (refresh).
 * El watermark T0 (inicio de la carga) permite el switch-over sin huecos hacia el delta.
 * seq = 0: en el MERGE cualquier delta (seq > 0) capturado desde T0 gana sobre la foto inicial.
 */
%dw 2.0
import * from dw::cdc::Envelope
output application/java
var rows = read(payload.Z_CDC_READ_TABLE.export.ET_DATA default "[]", "application/json")
var p = vars.partition
---
rows map ((row, i) -> envelope(
    { system: Mule::p("sap.systemId"), client: Mule::p("sap.client"), mechanism: "SNAPSHOT", table: p.table },
    "R",
    { (p.keys map ((k) -> { (k): row[k] })) },
    0,
    "T0:" ++ Mule::p("load.t0"),
    row,
    null,
    null,
    null,
    p.id ++ "|" ++ (vars.runId default "run") ++ "|" ++ ((vars.offset + i) as String)
))
