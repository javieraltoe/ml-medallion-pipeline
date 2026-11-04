# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Controles de calidad
# MAGIC
# MAGIC Cada control es una consulta que devuelve un número comparado contra un umbral.
# MAGIC Los resultados se guardan en `ml_audit.dq_results`; si falla un control de severidad
# MAGIC `error`, el notebook lanza una excepción y **el job queda en rojo**.

# COMMAND ----------

import os
import sys

NB_DIR = os.getcwd()
for path in (NB_DIR, os.path.join(os.path.dirname(NB_DIR), "src")):
    if path not in sys.path:
        sys.path.insert(0, path)

import operator

from pyspark.sql import functions as F

from dbx_common import get_params

p = get_params(dbutils)
T = p.table
d, dk = p.d, p.date_key

# (capa, nombre, dimensión, severidad, sql, operador, umbral, pasa_si_nulo)
CHECKS = [
    (
        "silver",
        "silver_all_sites_loaded",
        "completeness",
        "error",
        f"SELECT count(DISTINCT site_id) FROM {T('silver', 'item_snapshots')} WHERE snapshot_date = {d}",
        "gte",
        len(p.sites),
        False,
    ),
    (
        "silver",
        "silver_unique_item_per_day",
        "uniqueness",
        "error",
        f"""SELECT count(*) FROM (SELECT item_id FROM {T("silver", "item_snapshots")}
         WHERE snapshot_date = {d} GROUP BY item_id HAVING count(*) > 1)""",
        "eq",
        0,
        False,
    ),
    (
        "silver",
        "silver_price_positive",
        "validity",
        "error",
        f"SELECT count(*) FROM {T('silver', 'item_snapshots')} WHERE snapshot_date = {d} AND price <= 0",
        "eq",
        0,
        False,
    ),
    (
        "silver",
        "silver_reconciliation_with_bronze",
        "reconciliation",
        "error",
        f"""SELECT (SELECT count(DISTINCT item_id) FROM {T("bronze", "ml_items_raw")}
                 WHERE ingestion_date = {d} AND item_id IS NOT NULL)
              - (SELECT count(DISTINCT item_id) FROM (
                     SELECT item_id FROM {T("silver", "item_snapshots")} WHERE snapshot_date = {d}
                     UNION
                     SELECT item_id FROM {T("silver", "items_quarantine")}
                     WHERE snapshot_date = {d} AND item_id IS NOT NULL))""",
        "eq",
        0,
        False,
    ),
    (
        "silver",
        "silver_quarantine_ratio",
        "validity",
        "error",
        f"""SELECT (SELECT count(*) FROM {T("silver", "items_quarantine")} WHERE snapshot_date = {d})
              / nullif((SELECT count(*) FROM {T("bronze", "ml_items_raw")} WHERE ingestion_date = {d}), 0)""",
        "lte",
        0.10,
        False,
    ),
    (
        "silver",
        "silver_volume_vs_7d_avg",
        "volume",
        "error",
        f"""WITH daily AS (
             SELECT site_id, snapshot_date, count(*) AS n FROM {T("silver", "item_snapshots")}
             WHERE snapshot_date BETWEEN date_sub({d}, 7) AND {d} GROUP BY site_id, snapshot_date),
         cmp AS (
             SELECT t.site_id, t.n / avg(h.n) AS ratio
             FROM daily t JOIN daily h ON h.site_id = t.site_id AND h.snapshot_date < t.snapshot_date
             WHERE t.snapshot_date = {d} GROUP BY t.site_id, t.n)
         SELECT min(ratio) FROM cmp""",
        "gte",
        0.5,
        True,
    ),
    (
        "gold",
        "gold_fact_matches_silver_rows",
        "reconciliation",
        "error",
        f"""SELECT abs((SELECT count(*) FROM {T("gold", "fact_item_daily")} WHERE date_key = {dk})
                  - (SELECT count(*) FROM {T("silver", "item_snapshots")} WHERE snapshot_date = {d}))""",
        "eq",
        0,
        False,
    ),
    (
        "gold",
        "gold_fact_matches_silver_amounts",
        "reconciliation",
        "error",
        f"""SELECT abs(coalesce((SELECT sum(price_usd) FROM {T("gold", "fact_item_daily")} WHERE date_key = {dk}), 0)
                  - coalesce((SELECT sum(price_usd) FROM {T("silver", "item_snapshots")}
                              WHERE snapshot_date = {d}), 0))""",
        "lte",
        0.01,
        False,
    ),
    (
        "gold",
        "gold_scd2_one_current_version",
        "uniqueness",
        "error",
        f"""SELECT count(*) FROM (SELECT seller_id FROM {T("gold", "dim_seller")}
         GROUP BY seller_id HAVING count_if(is_current) <> 1)""",
        "eq",
        0,
        False,
    ),
    (
        "gold",
        "gold_scd2_no_overlapping_versions",
        "consistency",
        "error",
        f"""SELECT count(*) FROM {T("gold", "dim_seller")} a JOIN {T("gold", "dim_seller")} b
           ON a.seller_id = b.seller_id AND a.seller_sk < b.seller_sk
          AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to""",
        "eq",
        0,
        False,
    ),
    (
        "gold",
        "gold_fact_points_to_valid_seller_version",
        "integrity",
        "error",
        f"""SELECT count(*) FROM {T("gold", "fact_item_daily")} f
         JOIN {T("gold", "dim_seller")} v ON v.seller_sk = f.seller_sk
         WHERE f.date_key = {dk} AND v.valid_from <= {d} AND NOT ({d} < v.valid_to)""",
        "eq",
        0,
        False,
    ),
    (
        "gold",
        "gold_mart_country_coverage",
        "completeness",
        "error",
        f"SELECT count(DISTINCT site_id) FROM {T('gold', 'mart_category_daily')} WHERE full_date = {d}",
        "gte",
        len(p.sites),
        False,
    ),
]
OPS = {"eq": operator.eq, "lte": operator.le, "gte": operator.ge}

# COMMAND ----------

results = []
for layer, name, dimension, severity, sql, op, threshold, pass_if_null in CHECKS:
    raw = spark.sql(sql).first()[0]
    observed = None if raw is None else float(raw)
    if observed is None:
        passed = pass_if_null
        message = "Sin datos para comparar" if passed else "El control no devolvió valor"
    else:
        passed = OPS[op](round(observed, 6), float(threshold))
        message = f"observado={observed:g} {op} {threshold}"
    results.append(
        (p.run_id, p.run_date, layer, name, dimension, severity, passed, observed, float(threshold), message)
    )

df = spark.createDataFrame(
    results,
    "run_id string, run_date date, layer string, check_name string, dimension string, severity string, "
    "passed boolean, observed double, threshold double, message string",
).withColumn("checked_at", F.current_timestamp())
df.write.mode("append").saveAsTable(T("audit", "dq_results"))
display(df.select("layer", "check_name", "dimension", "passed", "observed", "threshold", "message"))

# COMMAND ----------

failed = [r for r in results if not r[6] and r[5] == "error"]
if failed:
    raise Exception(f"{len(failed)} control(es) de calidad fallaron: " + ", ".join(r[3] for r in failed))
print(f"Calidad OK: {len(results)} controles aprobados")
