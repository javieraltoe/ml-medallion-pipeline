# Lakehouse en Databricks Free Edition + Unity Catalog

La carpeta [`databricks_lakehouse/`](../databricks_lakehouse) reimplementa el pipeline Medallion en
**Databricks** con **PySpark, Delta Lake y Unity Catalog**, reutilizando la fuente de datos, el
contrato de esquema y los seeds del proyecto local.

| Notebook | Qué hace |
|---|---|
| `00_setup` | Crea esquemas, volumen y tablas Delta en Unity Catalog (idempotente) |
| `01_bronze` | Genera los datos del día, los escribe como JSONL en un **volumen** y los carga con `INSERT INTO ... REPLACE WHERE`. Valida el contrato |
| `02_silver` | `from_json` con esquema explícito, reglas → cuarentena, USD, `row_number()` y `MERGE` con `WHEN NOT MATCHED BY SOURCE` |
| `03_gold` | Dimensiones, **SCD tipo 2 con un único `MERGE`**, hechos *point-in-time* y mart |
| `04_quality` | 12 controles de calidad → `ml_audit.dq_results`; si falla uno, el job queda en rojo |
| `05_gobierno_unity_catalog` | Comentarios, tags, permisos, vista dinámica con datos enmascarados y linaje |
| `99_backfill` | Procesa un rango de fechas en orden |

## Equivalencias con la versión local

| Local (Docker) | Databricks |
|---|---|
| `data/lake/bronze/...` | Volumen `workspace.ml_bronze.landing` |
| Esquemas de Postgres | `workspace.ml_bronze` / `ml_silver` / `ml_gold` / `ml_audit` |
| pandas por chunks | PySpark (distribuido, sin cambiar la lógica) |
| Delete + insert por partición | `INSERT INTO ... REPLACE WHERE` sobre Delta |
| `MERGE` + `DELETE` aparte | `MERGE ... WHEN NOT MATCHED BY SOURCE THEN DELETE` |
| `BIGSERIAL` en `dim_seller` | Columna `GENERATED ALWAYS AS IDENTITY` |
| Índices | *Liquid clustering* (`CLUSTER BY`) |
| DAG de Airflow | Job de Databricks (Lakeflow Jobs) con 4 tareas |
| `_run_id`, `source_file` | Además, linaje automático de Unity Catalog (tabla y columna) |

## Paso a paso

### 1. Traer el repositorio a Databricks

1. En el menú izquierdo: **Workspace** → tu carpeta de usuario → **Create** → **Git folder**.
2. **Git repository URL:** `https://github.com/javieraltoe/ml-medallion-pipeline.git`
   (el repo es público, no pide credenciales) → **Create Git folder**.
3. Para traer cambios nuevos más adelante: abrir el Git folder → botón con el nombre de la rama
   → **Pull**.

### 2. Crear las tablas

1. Abrir `databricks_lakehouse/00_setup`.
2. Arriba a la derecha, **Connect** → **Serverless**.
3. **Run all**. Al final lista las tablas de cada esquema.

> El catálogo por defecto es `workspace`, que ya existe en Free Edition. Para usar otro, cambiá el
> widget `catalog` en todos los notebooks.

### 3. Cargar datos

**Un solo día:** abrir `01_bronze`, `02_silver`, `03_gold` y `04_quality` en orden, poner la misma
fecha en el widget `run_date` (vacío = hoy) y ejecutar cada uno.

**Varios días (recomendado):** abrir `99_backfill` y ejecutarlo. El rango por defecto
(26/10 al 04/11) incluye los cambios de vendedores del 28/10 y del 30/10, así que la SCD2 tiene
versiones para mostrar. Tarda varios minutos: cada día corre los cuatro notebooks.

### 4. Gobierno y linaje

1. Ejecutar `05_gobierno_unity_catalog` una vez.
2. **Catalog** → `workspace` → `ml_gold` → `mart_category_daily` → pestaña **Lineage** →
   **See lineage graph**. Se ve el recorrido desde `ml_items_raw` hasta el mart.
3. En la pestaña **Overview** de cada tabla aparecen los comentarios y tags.

### 5. Job diario

1. **Jobs & Pipelines** → **Create** → **Job**. Nombre: `ml_medallion_daily`.
2. Primera tarea:
   - *Task name:* `bronze` · *Type:* Notebook
   - *Source:* **Workspace** → elegir `databricks_lakehouse/01_bronze` dentro de tu Git folder
   - *Compute:* Serverless
   - Alternativa: *Source* **Git provider** (URL del repo, rama `main`), así el job siempre usa
     lo último de GitHub. Puede pedirte configurar una credencial de GitHub en *Settings → Linked accounts*.
3. Agregar tres tareas más con el mismo esquema, cada una dependiendo de la anterior:
   `silver` (`02_silver`), `gold` (`03_gold`), `quality` (`04_quality`).
4. En **Job parameters** agregar:

   | Clave | Valor |
   |---|---|
   | `catalog` | `workspace` |
   | `sites` | `MLA,MLB,MLM` |
   | `run_date` | `{{job.start_time.iso_date}}` |

5. **Schedules & Triggers** → diario. En Free Edition conviene pausarlo cuando no se use
   (hay un máximo de 5 tareas en simultáneo por cuenta).
6. **Run now** y mirar el grafo de tareas.

### 6. (Opcional) Dashboard en Databricks

**Dashboards** → **Create dashboard** → **Data** → **Create from SQL**:

```sql
SELECT full_date, country, category_name, median_price_usd, pct_free_shipping
FROM workspace.ml_gold.mart_category_daily
```

Con eso se arman gráficos de barras por país y categoría, igual que en Streamlit.

## Consultas para explorar (SQL editor)

```sql
-- Historial de un vendedor (SCD2)
SELECT seller_id, seller_sk, nickname, is_official_store, valid_from, valid_to, is_current
FROM workspace.ml_gold.dim_seller
WHERE seller_id IN (SELECT seller_id FROM workspace.ml_gold.dim_seller GROUP BY 1 HAVING count(*) > 1)
ORDER BY seller_id, valid_from;

-- Historia de la tabla Delta: cada MERGE/REPLACE queda versionado
DESCRIBE HISTORY workspace.ml_silver.item_snapshots;

-- Time travel: cómo estaba la tabla en una versión anterior
SELECT count(*) FROM workspace.ml_silver.item_snapshots VERSION AS OF 1;

-- Controles de calidad
SELECT run_date, layer, check_name, passed, observed, threshold
FROM workspace.ml_audit.dq_results ORDER BY checked_at DESC;
```

## Problemas frecuentes

| Error | Solución |
|---|---|
| `ModuleNotFoundError: dbx_common` o `ml_pipeline` | Abrir los notebooks **desde el Git folder** (no copiados sueltos) |
| `SCHEMA_NOT_FOUND` / `TABLE_OR_VIEW_NOT_FOUND` | Ejecutar primero `00_setup` con el mismo `catalog` |
| `CATALOG_NOT_FOUND: workspace` | Ver en **Catalog** cómo se llama tu catálogo y ponerlo en el widget `catalog` |
| Falla `04_quality` | Es el comportamiento esperado si un dato está mal: mirar qué control falló en la tabla mostrada |

## Cómo contarlo en una entrevista

> "Además de la versión local, implementé el lakehouse en Databricks con Unity Catalog. Bronze
> aterriza archivos en un volumen y los carga con `REPLACE WHERE`; Silver usa PySpark y un `MERGE`
> de Delta que también borra lo que ya no viene; Gold tiene la dimensión vendedor como SCD2 resuelta
> con un solo `MERGE`. Lo orquesto con un Job, y en Unity Catalog documenté las tablas, apliqué
> permisos de sólo lectura sobre Gold, una vista que enmascara datos sensibles, y uso el linaje
> automático para ver de dónde sale cada columna del mart."
