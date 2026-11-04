# Cómo se llevaría este proyecto a Databricks + Unity Catalog

El diseño es portable a propósito: Medallion, particiones estilo Hive, `MERGE` e idempotencia
son los mismos conceptos en Databricks. Este documento sirve para responder
“¿cómo usarías Databricks y Unity Catalog?” con un ejemplo concreto.

## Equivalencias

| En este repo | En Databricks |
|---|---|
| `data/lake/bronze/.../site_id=MLA/ingestion_date=.../*.jsonl` | Volumen de Unity Catalog o bucket S3/ADLS; ingesta con **Auto Loader** (`cloudFiles`) a una tabla Delta |
| Esquemas `bronze`, `silver`, `gold` de Postgres | `catalog.schema`: `ml_prod.bronze`, `ml_prod.silver`, `ml_prod.gold` |
| `MERGE INTO silver.item_snapshots ...` | `MERGE INTO` sobre Delta (misma sintaxis) |
| pandas por chunks | PySpark (escala horizontal sin cambiar la lógica) |
| `quality/checks.py` | Expectations de **Lakeflow Declarative Pipelines** (ex Delta Live Tables) o checks propios |
| `contracts/*.json` | Esquema esperado en Auto Loader (`schemaHints`, `rescuedDataColumn`) |
| DAG de Airflow | Airflow con `DatabricksRunNowOperator`, o **Lakeflow Jobs** |
| `_run_id`, `source_file` | Lineage automático de Unity Catalog a nivel tabla y columna |

## Unity Catalog: gobierno

```sql
CREATE CATALOG IF NOT EXISTS ml_prod;
CREATE SCHEMA IF NOT EXISTS ml_prod.gold;

-- Analistas: sólo lectura sobre Gold
GRANT USE CATALOG ON CATALOG ml_prod TO `analistas`;
GRANT USE SCHEMA, SELECT ON SCHEMA ml_prod.gold TO `analistas`;

-- El pipeline (service principal) escribe en todas las capas
GRANT ALL PRIVILEGES ON CATALOG ml_prod TO `sp-ml-pipeline`;

-- Documentación en el catálogo
COMMENT ON TABLE ml_prod.gold.fact_item_daily IS 'Grano: publicación x día. Precios en moneda local y USD.';
```

Con eso se obtiene en un solo lugar: permisos, descubrimiento (búsqueda en el catálogo),
auditoría de accesos y lineage desde la tabla Bronze hasta el dashboard.

> Si en la entrevista preguntan por experiencia en Databricks y sólo lo conocés
> conceptualmente, decilo así y usá esta tabla para mostrar que entendés el mapeo.
