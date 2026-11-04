# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: modelo estrella, SCD tipo 2 y data mart (Spark SQL sobre Delta)
# MAGIC
# MAGIC * Dimensiones: sitio, categoría, fecha y **vendedor con historial (SCD2)** usando un único
# MAGIC   `MERGE` de Delta (patrón "union de filas a cerrar + filas a insertar").
# MAGIC * Hechos con referencia *point-in-time* a la versión del vendedor vigente ese día.
# MAGIC * Mart por país, categoría y día.
# MAGIC
# MAGIC Patrón de carga de hechos y mart: `INSERT INTO ... REPLACE WHERE` por fecha (idempotente).

# COMMAND ----------

import os
import sys

NB_DIR = os.getcwd()
REPO_ROOT = os.path.dirname(NB_DIR)
for path in (NB_DIR, os.path.join(REPO_ROOT, "src")):
    if path not in sys.path:
        sys.path.insert(0, path)

import csv

from dbx_common import SITES, get_params, log_step

p = get_params(dbutils)
print(p)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensiones sitio, categoría y fecha

# COMMAND ----------

site_rows = [(s, m["country"], m["country_code"], m["currency"]) for s, m in SITES.items()]
spark.createDataFrame(
    site_rows, "site_id string, country string, country_code string, currency_id string"
).createOrReplaceTempView("src_sites")
spark.sql(f"""
    MERGE INTO {p.table("gold", "dim_site")} t USING src_sites s ON t.site_id = s.site_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Categorías: seed del repo + las que aparezcan nuevas quedan "Sin clasificar"
with open(os.path.join(REPO_ROOT, "seeds", "categories.csv"), encoding="utf-8") as fh:
    seed_rows = [(r["category_id"], r["site_id"], r["category_name"]) for r in csv.DictReader(fh)]
spark.createDataFrame(
    seed_rows, "category_id string, site_id string, category_name string"
).createOrReplaceTempView("src_categories")
spark.sql(f"""
    MERGE INTO {p.table("gold", "dim_category")} t
    USING (
        SELECT category_id, site_id, category_name FROM src_categories
        UNION ALL
        SELECT DISTINCT s.category_id, s.site_id, concat('Sin clasificar (', s.category_id, ')')
        FROM {p.table("silver", "item_snapshots")} s
        LEFT ANTI JOIN src_categories c ON c.category_id = s.category_id
        WHERE s.snapshot_date = {p.d}
    ) s
    ON t.category_id = s.category_id
    WHEN MATCHED AND s.category_name NOT LIKE 'Sin clasificar%' THEN UPDATE SET category_name = s.category_name
    WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
    MERGE INTO {p.table("gold", "dim_date")} t
    USING (
        SELECT CAST(date_format({p.d}, 'yyyyMMdd') AS INT) AS date_key,
               {p.d} AS full_date,
               CAST(year({p.d}) AS SMALLINT) AS year,
               CAST(quarter({p.d}) AS SMALLINT) AS quarter,
               CAST(month({p.d}) AS SMALLINT) AS month,
               CAST(day({p.d}) AS SMALLINT) AS day,
               CAST(weekday({p.d}) + 1 AS SMALLINT) AS day_of_week,
               weekday({p.d}) >= 5 AS is_weekend
    ) s
    ON t.date_key = s.date_key
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensión vendedor: SCD tipo 2 con un solo MERGE
# MAGIC
# MAGIC La fuente del MERGE tiene dos tipos de fila:
# MAGIC * `merge_key = seller_id`: matchea la versión vigente → si cambió, **se cierra** (`valid_to`, `is_current = false`).
# MAGIC   Si es un vendedor nuevo no matchea → **se inserta**.
# MAGIC * `merge_key = NULL` (sólo vendedores que cambiaron): nunca matchea → **se inserta la versión nueva**.

# COMMAND ----------

dim_seller = p.table("gold", "dim_seller")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW stg_seller AS
    SELECT seller_id, site_id, nickname, is_official_store,
           md5(concat_ws('|', nickname, CAST(is_official_store AS STRING))) AS row_hash
    FROM {p.table("silver", "seller_daily")}
    WHERE snapshot_date = {p.d}
""")

scd2 = spark.sql(f"""
    MERGE INTO {dim_seller} AS t
    USING (
        SELECT s.seller_id AS merge_key, s.* FROM stg_seller s
        UNION ALL
        SELECT NULL AS merge_key, s.*
        FROM stg_seller s
        JOIN {dim_seller} d
          ON d.seller_id = s.seller_id AND d.is_current
         AND d.row_hash <> s.row_hash AND d.valid_from < {p.d}
    ) AS src
    ON t.seller_id = src.merge_key AND t.is_current
    -- Cambió y la versión vigente es anterior a hoy: se cierra
    WHEN MATCHED AND t.row_hash <> src.row_hash AND t.valid_from < {p.d} THEN
        UPDATE SET valid_to = {p.d}, is_current = false
    -- Re-ejecución del mismo día con datos distintos: se corrige la versión de hoy
    WHEN MATCHED AND t.row_hash <> src.row_hash AND t.valid_from = {p.d} THEN
        UPDATE SET nickname = src.nickname, is_official_store = src.is_official_store, row_hash = src.row_hash
    -- Vendedor nuevo, o versión nueva de uno que cambió
    WHEN NOT MATCHED THEN
        INSERT (seller_id, site_id, nickname, is_official_store, row_hash, valid_from, valid_to, is_current)
        VALUES (src.seller_id, src.site_id, src.nickname, src.is_official_store, src.row_hash,
                {p.d}, DATE'9999-12-31', true)
""")
display(scd2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hechos (point-in-time) y data mart

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW fact_batch AS
    SELECT date_key, item_id, site_id, category_id, seller_sk, seller_id, currency_id, price_local,
           price_usd, discount_pct, available_quantity, free_shipping, is_new, _run_id
    FROM (
        SELECT CAST(date_format(s.snapshot_date, 'yyyyMMdd') AS INT) AS date_key,
               s.item_id, s.site_id, s.category_id, d.seller_sk, s.seller_id, s.currency_id,
               s.price AS price_local, s.price_usd, s.discount_pct, s.available_quantity,
               s.free_shipping, s.condition = 'new' AS is_new, '{p.run_id}' AS _run_id,
               -- Versión vigente ese día; si es un backfill anterior a la primera versión, la más antigua
               row_number() OVER (
                   PARTITION BY s.item_id
                   ORDER BY (d.valid_from <= s.snapshot_date AND s.snapshot_date < d.valid_to) DESC,
                            d.valid_from
               ) AS rn
        FROM {p.table("silver", "item_snapshots")} s
        JOIN {dim_seller} d ON d.seller_id = s.seller_id
        WHERE s.snapshot_date = {p.d}
    )
    WHERE rn = 1
""")
spark.sql(f"""
    INSERT INTO {p.table("gold", "fact_item_daily")}
    REPLACE WHERE date_key = {p.date_key}
    SELECT * FROM fact_batch
""")

spark.sql(f"""
    INSERT INTO {p.table("gold", "mart_category_daily")}
    REPLACE WHERE full_date = {p.d}
    SELECT d.full_date, f.site_id, st.country, f.category_id, c.category_name,
           count(*) AS items,
           count(DISTINCT f.seller_id) AS sellers,
           CAST(avg(f.price_usd) AS DECIMAL(18,2)) AS avg_price_usd,
           CAST(percentile(f.price_usd, 0.5) AS DECIMAL(18,2)) AS median_price_usd,
           CAST(percentile(f.price_usd, 0.25) AS DECIMAL(18,2)) AS p25_price_usd,
           CAST(percentile(f.price_usd, 0.75) AS DECIMAL(18,2)) AS p75_price_usd,
           CAST(100 * avg(CASE WHEN f.free_shipping THEN 1 ELSE 0 END) AS DECIMAL(5,2)) AS pct_free_shipping,
           CAST(avg(f.discount_pct) AS DECIMAL(6,2)) AS avg_discount_pct,
           '{p.run_id}' AS _run_id
    FROM {p.table("gold", "fact_item_daily")} f
    JOIN {p.table("gold", "dim_date")} d ON d.date_key = f.date_key
    JOIN {p.table("gold", "dim_site")} st ON st.site_id = f.site_id
    JOIN {p.table("gold", "dim_category")} c ON c.category_id = f.category_id
    WHERE d.full_date = {p.d}
    GROUP BY d.full_date, f.site_id, st.country, f.category_id, c.category_name
""")

# COMMAND ----------

facts = (
    spark.sql(f"SELECT count(*) AS n FROM {p.table('gold', 'fact_item_daily')} WHERE date_key = {p.date_key}")
    .first()
    .n
)
versions = spark.sql(
    f"SELECT count(*) AS versiones, count(DISTINCT seller_id) AS vendedores FROM {dim_seller}"
).first()
log_step(
    spark,
    p,
    "gold_build",
    rows_out=facts,
    details={"seller_versions": versions.versiones, "sellers": versions.vendedores},
)
print(
    f"Gold OK · hechos del día: {facts} · versiones de vendedor: {versions.versiones} "
    f"para {versions.vendedores} vendedores"
)

display(
    spark.sql(f"""
    SELECT country, category_name, items, median_price_usd, pct_free_shipping
    FROM {p.table("gold", "mart_category_daily")}
    WHERE full_date = {p.d}
    ORDER BY category_name, country
""")
)
