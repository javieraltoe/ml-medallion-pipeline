# Runbook: qué hacer cuando algo falla

Regla general: **un DAG en verde no garantiza datos correctos**. Por eso existen los
controles; y cuando uno falla, primero se entiende la causa y después se re-ejecuta.
Como todas las tareas son idempotentes, re-ejecutar (*Clear* en la UI) es seguro.

## 1. Falla `silver_volume_vs_7d_avg` (llegaron muchas menos filas)

1. ¿Es un país o todos? `SELECT * FROM audit.vw_dq_latest WHERE check_name LIKE '%volume%'`
   y la consulta 9 de `sql/analysis/consultas_demo.sql`.
2. Revisar `audit.pipeline_runs` (paso `bronze_extract`): ¿bajó `rows_in` (origen) o se
   perdieron filas entre Bronze y Silver (filtros, rechazos)?
3. Si es el origen: revisar respuestas de la API en el log de la tarea (paginación, 403/429,
   cambios en los términos de búsqueda).
4. Si es real (ej. feriado, menos publicaciones), documentarlo y re-ejecutar con un umbral
   ajustado (`DQ_VOLUME_DROP_THRESHOLD`) sólo para esa fecha.

## 2. Aparecen duplicados

1. ¿Dónde aparecen? En Bronze son esperables (consulta 5). En Silver no deberían: lo impide
   la PK `(item_id, snapshot_date)` y lo verifica `silver_unique_item_per_day`.
2. Causas típicas: paginación solapada, el mismo ítem en dos términos de búsqueda, un join
   uno-a-muchos nuevo, o una carga no idempotente.
3. Detectarlos: `GROUP BY clave HAVING count(*) > 1`. Resolverlos: `ROW_NUMBER()` con un
   desempate determinista + `MERGE` por clave de negocio.

## 3. Falla el contrato (`SchemaContractError`)

1. El crudo **ya está guardado** en Bronze: `SELECT payload FROM bronze.ml_items_raw WHERE
   ingestion_date = '...' LIMIT 5`.
2. El mensaje del error dice qué campo falta o cambió de tipo y en qué % de registros.
3. Si el cambio es legítimo: actualizar `contracts/ml_search_item.json` (subir la versión),
   el mapeo en `transform/normalize.py` y `docs/source_to_target.md`; abrir un PR.
4. Re-ejecutar el día desde Bronze.

## 4. Falla `silver_quarantine_ratio`

`SELECT reason, count(*) FROM silver.items_quarantine WHERE snapshot_date = '...' GROUP BY 1`.
Un motivo nuevo dominante suele indicar un cambio en el origen (ej. precios que llegan como
texto) más que datos realmente malos.

## 5. 401 / 403 de la API

- `401`: token vencido; el cliente lo renueva solo. Si persiste, el refresh token pudo haber
  sido usado por otro proceso (son de un solo uso): generar uno nuevo (ver
  `docs/mercadolibre_api.md`).
- `403`: la app no tiene permiso para ese recurso. Revisar la configuración de la app.
- Mientras tanto se puede seguir trabajando con `ML_SOURCE_MODE=sample`.

## 6. Re-procesar un rango de fechas

```bash
make backfill START=2026-10-01 END=2026-10-15
```
