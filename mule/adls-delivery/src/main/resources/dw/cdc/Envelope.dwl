/**
 * Sobre CDC canónico — Artes Gráficas.
 * Módulo DataWeave compartido: se copia a src/main/resources/dw/cdc/ de cada app.
 * Uso:  import * from dw::cdc::Envelope
 */
%dw 2.0
import SHA1 from dw::Crypto

/** Timestamp UTC ISO-8601 sin milisegundos. */
fun utcNow(): String = (now() >> "UTC") as String { format: "yyyy-MM-dd'T'HH:mm:ss'Z'" }

/**
 * cdcId DETERMINISTA: misma posición en la fuente ⇒ mismo id.
 * idSeed identifica el cambio en su origen (delta link + página + fila, DOCNUM + segmento,
 * SEQ del log, corrida + offset). Si SAP reentrega o se repite un delta, el id se repite y
 * adls-delivery lo descarta como duplicado (solo si ya fue comprometido en el lake).
 */
fun cdcIdOf(src: Object, key: Object, op: String, idSeed: String): String =
    SHA1(write([src.system, src.client, src.mechanism, src.table, op, idSeed, key], "application/json", {indent: false}) as Binary {encoding: "UTF-8"})

/**
 * Construye el sobre CDC.
 * src: { system, client, mechanism: IDOC|ODP|TRIGGER|SNAPSHOT, table }
 */
fun envelope(src: Object, op: String, key: Object, seq: Number, watermark: String,
             after: Object | Null, before: Object | Null = null,
             correlationId: String | Null = null, changeTs: String | Null = null,
             idSeed: String | Null = null) = {
    cdcId: if (idSeed != null) cdcIdOf(src, key, op, idSeed) else uuid(),
    correlationId: correlationId default uuid(),
    source: src,
    op: op,
    key: key,
    changeTs: changeTs default utcNow(),
    seq: seq,
    watermark: watermark,
    before: before,
    after: after
}

/** Propiedades de mensaje Anypoint MQ (trazabilidad sin parsear el body). */
fun mqProperties(env: Object) = {
    correlationId: env.correlationId,
    cdcId: env.cdcId,
    table: env.source.table,
    op: env.op,
    mechanism: env.source.mechanism
}

/** Sobre de error para la DLQ (cdc.dlq). */
fun errorEnvelope(err: Any, original: Any, stage: String, app: String, attempts: Number = 1) = {
    errorType: if (err.errorType?) (err.errorType.namespace default "") ++ ":" ++ (err.errorType.identifier default "") else "UNKNOWN",
    description: err.description default "",
    stage: stage,
    attempts: attempts,
    firstFailureTs: utcNow(),
    app: app,
    correlationId: original.correlationId default null,
    cdcId: original.cdcId default null,
    original: original
}

/** Evento de auditoría/log estructurado por etapa: CAPTURED · PUBLISHED · LANDED · COMMITTED · DUPLICATE · DLQ */
fun stageEvent(stage: String, app: String, data: Object) = {
    ts: utcNow(),
    app: app,
    stage: stage
} ++ data
