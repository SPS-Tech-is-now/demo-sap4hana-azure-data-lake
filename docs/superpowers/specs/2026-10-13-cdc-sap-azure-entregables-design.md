# Spec — Entregables CDC SAP → Azure (Artes Gráficas)

**Fecha:** 2026-10-13 · **Fuente de verdad del diseño:** `Planteamiento_CDC_SAP_Azure_MuleSoft.md` (raíz del proyecto)

## Entregable 1 — Planteamiento (Markdown)
Ya escrito: `Planteamiento_CDC_SAP_Azure_MuleSoft.md`. Es la especificación funcional y técnica; los entregables 2 y 3 deben ser consistentes con él (mismas cifras, nombres de apps, endpoints, sobre CDC).

## Entregable 2 — HTML de presentación al cliente
- **Archivo:** `html/ArtesGraficas_CDC_MuleSoft.html`, single-file (CSS/JS inline, sin dependencias salvo Google Fonts), responsivo, modo claro/oscuro.
- **Audiencia:** dirección de TI y negocio de Artes Gráficas; se presenta en reunión y se comparte como link/archivo.
- **Marca:** SPS (SP Solutions) como autor — logo `C:\rolando\SPS\2019\Nueva Imagen\SPS_Nueva Imagen\SPS_Nueva Imagen\Logos\SPS.png` **embebido en base64** (el HTML debe funcionar fuera de esta máquina), link a www.spsolutions.com.mx en header/footer, siguiendo el design system de la skill `enterprise-proposal-html` (formato Status/Planning Report). Artes Gráficas aparece como cliente en header, hero y footer ("Confidencial · Artes Gráficas × SPS · 2026"); sin logo del cliente (no disponible).
- **Secciones:**
  1. Hero con mensaje clave y KPIs (63 tablas, ~15K cambios/día hábil, 4 niveles de reintento, 0 pérdidas).
  2. El reto: volumetría (top 5 tablas, barras) y hallazgo ECC on HANA.
  3. Arquitectura animada (SVG): fuentes SAP → apps de captura → Anypoint MQ → adls-delivery → ADLS; partículas recorriendo el flujo.
  4. Estrategia por dominio (tarjetas con mecanismo, latencia, tablas).
  5. **Simulador interactivo de garantías:** botones "Caída de Azure", "Throttling 429", "Mensaje corrupto", "Duplicado" → animan el camino del mensaje por los 4 niveles de reintento / DLQ / idempotencia.
  6. Commit atómico en ADLS paso a paso (_tmp → rename → manifest → ACK).
  7. Trazabilidad: timeline de un correlationId por etapas + vista de conciliación.
  8. Carga inicial y switch-over T0.
  9. Roadmap F0–F3, riesgos, por qué MuleSoft.
- **Datos:** todas las cifras salen del planteamiento (no inventar nuevas).

## Entregable 3 — Código Mule 4 de referencia
- **Ubicación:** `mule/` con un proyecto Maven por app; Mule 4.6+, Java 17.
- **Proyectos y archivos clave:**
  - `cdc-common/` — `cdc-envelope.json` (JSON Schema), DataWeave reusable `cdc-envelope.dwl`, `error-envelope.dwl`.
  - `sap-odp-cdc/` — scheduler por suscripción, HTTP OData con `Prefer: odata.track-changes`, paginación `__next`, delta token en Object Store v2 confirmado al final, publish a Anypoint MQ con propiedades `correlationId`, `table`, `op`.
  - `sap-idoc-cdc/` — SAP Connector document listener transaccional (tRFC), DataWeave IDoc DEBMAS/MATMAS → sobre CDC, publish MQ; error → excepción a SAP (IDoc queda para reintento).
  - `hana-trigger-cdc/` — DDL de `ZCDC_LOG` + trigger ejemplo (VBAP), flujo DB select por watermark + publish + update `PROCESSED` en transacción local.
  - `adls-delivery/` — subscriber MQ ACK manual, idempotent-message-validator, aggregator (size/time) acumulando ackTokens, ADLS DFS create/append/flush/rename + manifest, until-successful selectivo, ack/nack, error handler → DLQ con error envelope, log por etapa.
  - `cdc-control-api/` — RAML + flujos `/trace`, `/health/lag`, `/reconcile`, `/resync`, `/dlq/{queue}/replay`.
  - Cada proyecto: `pom.xml`, `mule-artifact.json`, `src/main/resources/config-*.yaml` con secure properties, MUnit de muestra en `adls-delivery` (ack tras commit, nack ante 503, DLQ tras error no reintentable).
  - `sap-initial-load/` — versión ligera: Batch Job sobre particiones (tabla × BUKRS × GJAHR) con estado por partición en Object Store (reanudable), `maxConcurrency` limitado, salida al mismo sobre CDC con `op = R` hacia MQ, `onComplete` con reporte. Se incluye porque la carga histórica (~75 GB) es la duda natural del cliente y Batch Job es una capacidad distintiva de Mule.
- **Fuera de alcance:** `sap-rfc-snapshot` solo en README (es un caso trivial de RFC + `op = R`).
- **Verificación:** XML bien formado y DataWeave revisado; no se compila contra Exchange (no hay credenciales/conectores locales) — se declara explícitamente.

## Notas
- La carpeta no es repositorio git; no se hace commit.
