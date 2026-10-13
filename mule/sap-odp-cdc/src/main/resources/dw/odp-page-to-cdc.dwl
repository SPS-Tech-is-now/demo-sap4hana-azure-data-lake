/**
 * Convierte una página OData (V2) de un delta ODP en sobres CDC.
 * ODP marca cada fila con ODQ_CHANGEMODE: C=create, U=update, D=delete, vacío=carga inicial.
 * Entrada: payload = respuesta OData { d: { results: [...], __next?, __delta? } }
 * Variables: vars.sub (suscripción), vars.watermark (delta link usado), vars.seqBase
 */
%dw 2.0
import * from dw::cdc::Envelope
output application/java

var src = {
    system: Mule::p("sap.systemId"),
    client: Mule::p("sap.client"),
    mechanism: "ODP",
    table: vars.sub.table
}
fun opOf(mode) = mode match {
    case "C" -> "I"
    case "U" -> "U"
    case "D" -> "D"
    else     -> "R"
}
fun clean(row) = row -- ["__metadata", "ODQ_CHANGEMODE", "ODQ_ENTITYCNTR"]
---
(payload.d.results default []) map ((row, i) -> do {
    var op = opOf(row.ODQ_CHANGEMODE default "")
    ---
    envelope(
        src,
        op,
        { (vars.sub.keys map ((k) -> { (k): row[k] })) },
        vars.seqBase + i,
        vars.watermark default "INIT",
        if (op == "D") null else clean(row),
        if (op == "D") clean(row) else null,
        null,
        null,
        (vars.watermark default "INIT") ++ "|p" ++ (vars.pages as String) ++ "|" ++ (i as String)
    )
})
