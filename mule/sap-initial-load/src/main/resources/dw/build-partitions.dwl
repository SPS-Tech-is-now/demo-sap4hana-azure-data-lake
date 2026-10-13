/**
 * Expande partitions.json en la lista de particiones: tabla × BUKRS × GJAHR.
 * Cada partición lleva su id (llave en Object Store) y su filtro WHERE.
 */
%dw 2.0
output application/java
var plan = readUrl("classpath://partitions.json", "application/json")
fun comps(t) = if (t.byCompany != null) plan.companies else ["ALL"]
fun years(t) = if (t.byYear != null) plan.years else ["ALL"]
fun where(t, c, y) = (
    (if (c != "ALL") [t.byCompany ++ " = '" ++ c ++ "'"] else []) ++
    (if (y != "ALL") [t.byYear ++ " = '" ++ y ++ "'"] else [])
) joinBy " AND "
---
flatten(flatten(plan.tables map ((t) ->
    comps(t) map ((c) ->
        years(t) map ((y) -> {
            id: t.table ++ "|" ++ c ++ "|" ++ y,
            table: t.table,
            domain: t.domain,
            keys: t.keys,
            company: c,
            year: y,
            filter: where(t, c, y)
        })
    )
)))
