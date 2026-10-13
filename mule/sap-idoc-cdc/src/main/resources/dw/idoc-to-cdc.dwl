/**
 * Paquete de IDocs de datos maestros (DEBMAS / CREMAS / MATMAS) → sobres CDC (uno por segmento-tabla).
 * Un mismo tRFC puede traer VARIOS IDocs (tamaño de paquete en WE20): se procesan todos.
 * MSGFN del segmento: 003 = borrado · 004 = modificación · 005/009/vacío = alta/reenvío.
 * El DOCNUM de cada IDoc es su correlationId (rastreable en WE02/BD87) y la semilla del cdcId:
 * si SAP reenvía el IDoc, los sobres conservan su cdcId y adls-delivery los descarta como duplicados.
 */
%dw 2.0
import * from dw::cdc::Envelope
output application/java

var idocs = payload[0].*IDOC default []

/** Segmento → tabla SAP y llave de negocio. */
var segmentMap = {
    E1KNA1M: { table: "KNA1", keys: ["KUNNR"] },
    E1KNVVM: { table: "KNVV", keys: ["KUNNR", "VKORG", "VTWEG", "SPART"] },
    E1KNVPM: { table: "KNVP", keys: ["KUNNR", "VKORG", "VTWEG", "SPART", "PARVW", "PARZA"] },
    E1LFA1M: { table: "LFA1", keys: ["LIFNR"] },
    E1MARAM: { table: "MARA", keys: ["MATNR"] },
    E1MAKTM: { table: "MAKT", keys: ["MATNR", "SPRAS"] },
    E1MARMM: { table: "MARM", keys: ["MATNR", "MEINH"] },
    E1MVKEM: { table: "MVKE", keys: ["MATNR", "VKORG", "VTWEG"] },
    E1MBEWM: { table: "MBEW", keys: ["MATNR", "BWKEY", "BWTAR"] }
}

fun opOf(msgfn) = msgfn match {
    case "003" -> "D"
    case "004" -> "U"
    else       -> "I"
}

/** Recorre recursivamente el IDoc y devuelve [{segment, fields, key}] para segmentos mapeados. */
fun collect(node, parentKey: Object = {}) =
    flatten(node pluck ((value, name) -> do {
        var seg = name as String
        ---
        if (segmentMap[seg]?) do {
            var fields = value filterObject ((v, k) -> not (v is Object))
            var key = parentKey ++ (fields filterObject ((v, k) -> segmentMap[seg].keys contains (k as String)))
            ---
            [{ segment: seg, fields: fields, key: key }] ++ (if (value is Object) collect(value, key) else [])
        }
        else if (value is Object) collect(value, parentKey)
        else []
    }))
---
flatten(idocs map ((idoc) -> do {
    var docnum = idoc.EDI_DC40.DOCNUM as String
    var rows = collect(idoc -- ["EDI_DC40"])
    ---
    rows map ((r, i) -> do {
        var op = opOf(r.fields.MSGFN default "")
        var src = { system: Mule::p("sap.systemId"), client: Mule::p("sap.client"), mechanism: "IDOC", table: segmentMap[r.segment].table }
        ---
        envelope(
            src,
            op,
            r.key filterObject ((v, k) -> segmentMap[r.segment].keys contains (k as String)),
            (docnum as Number) * 1000000 + i,
            "IDOC:" ++ docnum,
            if (op == "D") null else (r.fields -- ["MSGFN"]),
            if (op == "D") (r.fields -- ["MSGFN"]) else null,
            docnum,
            null,
            docnum ++ "|" ++ (i as String)
        )
    })
}))
