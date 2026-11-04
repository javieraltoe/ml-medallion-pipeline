# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Backfill: procesar un rango de fechas
# MAGIC
# MAGIC Ejecuta Bronze → Silver → Gold → Calidad para cada día del rango, **en orden** (el control
# MAGIC de volumen y el SCD2 dependen de procesar los días cronológicamente). Como todo es
# MAGIC idempotente, se puede volver a correr sobre días ya cargados.

# COMMAND ----------

from datetime import date, timedelta

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("start_date", "2026-10-26")
dbutils.widgets.text("end_date", "2026-11-04")
dbutils.widgets.text("sites", "MLA,MLB,MLM")

catalog = dbutils.widgets.get("catalog")
start = date.fromisoformat(dbutils.widgets.get("start_date"))
end = date.fromisoformat(dbutils.widgets.get("end_date"))
sites = dbutils.widgets.get("sites")
assert start <= end, "start_date debe ser <= end_date"

# COMMAND ----------

STEPS = ["./01_bronze", "./02_silver", "./03_gold", "./04_quality"]

day = start
while day <= end:
    args = {
        "catalog": catalog,
        "run_date": day.isoformat(),
        "sites": sites,
        "run_id": f"backfill__{day.isoformat()}",
    }
    for step in STEPS:
        dbutils.notebook.run(step, 1800, args)
    print(f"{day} OK")
    day += timedelta(days=1)

print("Backfill completo")
