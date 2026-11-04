# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup del lakehouse en Unity Catalog
# MAGIC
# MAGIC Crea los esquemas `ml_bronze`, `ml_silver`, `ml_gold`, `ml_audit`, el volumen de aterrizaje y
# MAGIC todas las tablas Delta. Es **idempotente**: se puede ejecutar las veces que haga falta.
# MAGIC
# MAGIC | Parámetro | Default | Descripción |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Catálogo de Unity Catalog (en Free Edition existe `workspace`) |

# COMMAND ----------

import os
import sys

NB_DIR = os.getcwd()
for path in (NB_DIR, os.path.join(os.path.dirname(NB_DIR), "src")):
    if path not in sys.path:
        sys.path.insert(0, path)

from dbx_common import bootstrap_sql, get_params

p = get_params(dbutils)
print(f"Catálogo: {p.catalog}")

# COMMAND ----------

with open(os.path.join(NB_DIR, "sql", "ddl.sql"), encoding="utf-8") as fh:
    statements = bootstrap_sql(fh.read(), p.catalog)

for stmt in statements:
    first_line = stmt.splitlines()[0]
    print(f"-> {first_line[:100]}")
    spark.sql(stmt)

print(f"\n{len(statements)} sentencias ejecutadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------

for layer in ("bronze", "silver", "gold", "audit"):
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {p.schema(layer)}").collect()]
    print(f"ml_{layer}: {', '.join(sorted(tables))}")

display(spark.sql(f"SHOW VOLUMES IN {p.schema('bronze')}"))
