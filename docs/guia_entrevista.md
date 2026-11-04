# Guía para presentar el proyecto en una entrevista

## Pitch de 60 segundos

> “Construí un pipeline diario que toma publicaciones de Mercado Libre de Argentina, Brasil y
> México y responde cuánto cuesta lo mismo en cada país en dólares. Está orquestado con
> Airflow 3 en Docker y sigue la arquitectura Medallion: en Bronze guardo el JSON crudo tal
> cual llega; en Silver limpio, deduplico con ROW_NUMBER, convierto a USD con una tasa
> fechada y cargo con MERGE; en Gold armo un modelo estrella y un data mart. Todas las tareas
> son idempotentes, así que puedo re-ejecutar o hacer backfill sin duplicar. Tiene contratos
> de esquema y controles de calidad que frenan la publicación si algo está mal, por ejemplo
> si llegan 80% menos filas. Tiene tests unitarios, un test end-to-end contra Postgres y CI en
> GitHub Actions.”

## Demo de 5 minutos

1. **README** en GitHub: diagrama de arquitectura (10 s).
2. **Airflow UI** (`localhost:8080`): el DAG con los grupos `bronze` → `silver` → `gold`,
   el mapeo por país y una corrida en verde. Mostrar los reintentos en *Task details*.
3. **VS Code + SQLTools**: `sql/analysis/consultas_demo.sql`
   - consulta 1 (mart por país) → el resultado de negocio,
   - consulta 5 (duplicados en Bronze) → “acá estaban, Silver los resolvió”,
   - consulta 7 (controles de calidad).
4. **Romperlo a propósito:** `make demo-volume-drop` → mostrar el error y la fila en
   `audit.dq_results`. Es el momento más fuerte de la demo.
5. **Código:** `sql/silver/merge_items.sql` (ROW_NUMBER + MERGE) y `dags/ml_medallion_daily.py`
   (“el DAG sólo orquesta; la lógica se testea sin Airflow”).

## Dónde está cada tema del simulacro en este repo

### Parte 1 · Opción múltiple

| # | Tema | Evidencia en el proyecto |
|---|------|--------------------------|
| 1 | ETL vs ELT | Silver es ETL (transformo en pandas y después cargo); Gold es ELT (cargo y transformo en SQL dentro del warehouse) |
| 2 | Último registro por entidad | `ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY record_seq DESC)` en `sql/silver/merge_items.sql`; vista `gold.vw_latest_item_price` |
| 3 | Archivo grande, poca memoria | `db.iter_chunks`: cursor del servidor que lee de a 5.000 filas (`silver.py`) |
| 4 | Qué es un DAG | `dags/ml_medallion_daily.py` |
| 5 | Tarea re-ejecutable sin duplicar | MERGE + PK + delete/insert por partición; `test_rerun_is_idempotent` |
| 6 | Validación de unicidad | Control `silver_unique_item_per_day` |
| 7 | Mapeo source-to-target | `docs/source_to_target.md` |
| 8 | Multimoneda | Se conserva `currency_id` + `usd_per_unit` + `price_usd`; tasa por fecha (`silver.fx_rates`) |
| 9 | Tabla de hechos | `gold.fact_item_daily`: métricas + claves a 4 dimensiones, grano publicación × día |
| 10 | Data lineage | `_run_id`, `source_file`, `payload_hash`; consulta 8 traza Gold → archivo crudo |
| 11 | Unity Catalog | `docs/databricks.md` |
| 12 | MERGE | `sql/silver/merge_items.sql`, `sql/silver/merge_fx_rates.sql` |
| 13 | Pipeline OK pero 80% menos filas | Control `silver_volume_vs_7d_avg` + `make demo-volume-drop` + `docs/runbook.md` |
| 14 | Data leakage | Snapshots diarios fechados: para entrenar un modelo se corta por `snapshot_date` (ver abajo) |
| 15 | Cambio de esquema | `contracts/ml_search_item.json` + `make demo-schema-break` |

### Parte 2 · Preguntas abiertas

**1. Pipeline para varios países.** Es exactamente este proyecto: una tarea por país con
dynamic task mapping, Bronze crudo, Silver unificado y deduplicado, Gold con métricas,
reconciliación de cantidades e importes, y moneda original + USD con tasa fechada.

**2. Optimizar una consulta lenta.** `EXPLAIN (ANALYZE, BUFFERS)` primero. En este repo:
índices por partición (`ix_snapshots_date_site`), filtros por fecha que permiten usar el
índice, `date_key` entero en la tabla de hechos. Medir antes y después y verificar que el
resultado sea idéntico.

**3. Duplicados.** `docs/runbook.md` sección 2: dónde nacen (origen, joins, reintentos),
cómo detectarlos (`GROUP BY ... HAVING`), cómo resolverlos (ROW_NUMBER + MERGE + PK) y cómo
evitar que vuelvan (control automático).

**4. Airflow.** Schedule diario, dependencias por capas, 2 reintentos cada 5 minutos,
`retries=0` en calidad, `AirflowFailException` ante cambio de esquema, callback de alertas,
backfill. **Ser honesto:** “lo usé en este proyecto con Airflow 3 en Docker”, no
“en producción” si no fue así.

**5. Migración entre ERPs.** Mismos artefactos que acá: mapeo source-to-target,
reglas por campo, cuarentena, reconciliación de conteos e importes, ejecución idempotente
que permite pilotos y re-ejecuciones antes del cutover.

**6. Databricks y Unity Catalog.** `docs/databricks.md`.

**7. Controles de calidad.** Tabla en el README: contrato, unicidad, completitud, validez,
consistencia, reconciliación, integridad referencial, volumen y frescura, con umbrales,
severidad y registro histórico.

**8. Datos para IA.** Con `silver.item_snapshots` se puede entrenar, por ejemplo, un modelo
que prediga bajas de precio. Las features de un día sólo pueden usar datos ≤ ese día; el
split train/validation/test es temporal (nunca aleatorio) para evitar *data leakage*; el
dataset se versiona por rango de `snapshot_date` + `_run_id`.

### Parte 3 · Ejercicios prácticos

| Ejercicio | Versión aplicada en el repo |
|-----------|-----------------------------|
| SQL: última venta por cliente | Consulta 3 de `consultas_demo.sql` y `gold.vw_latest_item_price` (con desempate) |
| Python: normalización de monedas | `normalize.add_usd_amount` + `test_add_usd_amount_fails_on_unknown_currency` |
| Airflow: DAG diario con reintentos | `dags/ml_medallion_daily.py` (`retries=2`, `retry_delay=5 min`) |

## Preguntas de seguimiento probables

- **¿Por qué Postgres y no un data warehouse cloud?** Para que corra gratis en cualquier
  laptop. El SQL es estándar (MERGE, ventanas) y se porta a Snowflake/BigQuery/Databricks.
- **¿Por qué datos sintéticos?** Mercado Libre exige OAuth desde 2025. El modo `sample` tiene
  la misma forma que la API, permite demos reproducibles y CI sin secretos; el modo `api`
  usa la API real con la misma lógica.
- **¿Qué pasa si corrés el mismo día dos veces?** Nada cambia: lo prueba un test de integración.
- **¿Qué mejorarías?** Parquet/Delta en almacenamiento de objetos, dbt para Gold, alertas a
  Slack, dashboard. Está en “Próximos pasos” del README.
