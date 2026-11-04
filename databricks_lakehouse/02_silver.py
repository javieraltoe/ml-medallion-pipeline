# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: limpiar, convertir a USD y deduplicar (PySpark + Delta MERGE)
# MAGIC
# MAGIC Misma lógica que la versión local (`src/ml_pipeline/silver.py`), escrita en PySpark:
# MAGIC
# MAGIC 1. Parsear el JSON crudo con un esquema explícito (`from_json`).
# MAGIC 2. Reglas de negocio → lo inválido va a `items_quarantine` con el motivo.
# MAGIC 3. Conversión a USD con la tasa vigente a la fecha.
# MAGIC 4. Deduplicar con `row_number()` sobre una ventana.
# MAGIC 5. `MERGE` en Delta, incluyendo `WHEN NOT MATCHED BY SOURCE ... DELETE` para sincronizar la partición.

# COMMAND ----------

import os
import sys

NB_DIR = os.getcwd()
for path in (NB_DIR, os.path.join(os.path.dirname(NB_DIR), "src")):
    if path not in sys.path:
        sys.path.insert(0, path)

from pyspark.sql import Window
from pyspark.sql import functions as F

from dbx_common import get_params, log_step

p = get_params(dbutils)
print(p)

ITEM_SCHEMA = """
    id STRING, site_id STRING, title STRING, category_id STRING,
    price DOUBLE, original_price DOUBLE, currency_id STRING, available_quantity INT,
    condition STRING, listing_type_id STRING, permalink STRING, official_store_id BIGINT,
    seller STRUCT<id: BIGINT, nickname: STRING>,
    shipping STRUCT<free_shipping: BOOLEAN, logistic_type: STRING>,
    attributes ARRAY<STRUCT<id: STRING, value_name: STRING>>
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parsear y aplanar

# COMMAND ----------

bronze = spark.table(p.table("bronze", "ml_items_raw")).where(
    f"ingestion_date = {p.d} AND site_id IN ({p.sites_sql})"
)

flat = bronze.withColumn("i", F.from_json("payload", ITEM_SCHEMA)).select(
    F.col("i.id").alias("item_id"),
    F.lit(p.run_date).cast("date").alias("snapshot_date"),
    F.col("site_id"),
    F.col("search_term"),
    F.expr("nullif(trim(i.title), '')").alias("title"),
    F.col("i.category_id").alias("category_id"),
    F.col("i.seller.id").alias("seller_id"),
    F.col("i.seller.nickname").alias("seller_nickname"),
    F.col("i.condition").alias("condition"),
    F.col("i.listing_type_id").alias("listing_type_id"),
    F.expr("try_element_at(filter(i.attributes, a -> a.id = 'BRAND'), 1).value_name").alias("brand"),
    F.upper("i.currency_id").alias("currency_id"),
    F.col("i.price").alias("price"),
    F.col("i.original_price").alias("original_price"),
    F.col("i.available_quantity").alias("available_quantity"),
    F.col("i.shipping.free_shipping").alias("free_shipping"),
    F.col("i.shipping.logistic_type").alias("logistic_type"),
    F.col("i.official_store_id").alias("official_store_id"),
    F.col("i.permalink").alias("permalink"),
    F.col("record_seq"),
    F.col("payload"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-3. Reglas de negocio y conversión a USD

# COMMAND ----------

# Tasa vigente: la más reciente con fecha <= día del snapshot.
rates = spark.sql(f"""
    SELECT currency_id, usd_per_unit FROM (
        SELECT currency_id, usd_per_unit,
               row_number() OVER (PARTITION BY currency_id ORDER BY rate_date DESC) AS rn
        FROM {p.table("silver", "fx_rates")}
        WHERE rate_date <= {p.d}
    ) WHERE rn = 1
""")

checked = flat.join(F.broadcast(rates), "currency_id", "left").withColumn(
    "reason",
    F.when(F.col("item_id").isNull(), "id_ausente")
    .when(F.col("title").isNull(), "titulo_ausente")
    .when(F.col("seller_id").isNull(), "vendedor_ausente")
    .when(F.col("category_id").isNull(), "categoria_ausente")
    .when(F.col("price").isNull(), "precio_nulo")
    .when(F.col("price") <= 0, "precio_no_positivo")
    .when(F.col("usd_per_unit").isNull(), "moneda_sin_tipo_de_cambio"),
)

quarantine = checked.where("reason IS NOT NULL").select(
    "snapshot_date",
    "site_id",
    "item_id",
    "reason",
    "payload",
    F.lit(p.run_id).alias("_run_id"),
    F.current_timestamp().alias("quarantined_at"),
)
quarantine.createOrReplaceTempView("quarantine_batch")
spark.sql(f"""
    INSERT INTO {p.table("silver", "items_quarantine")}
    REPLACE WHERE snapshot_date = {p.d} AND site_id IN ({p.sites_sql})
    SELECT * FROM quarantine_batch
""")

valid = (
    checked.where("reason IS NULL")
    .withColumn("price", F.col("price").cast("decimal(18,2)"))
    .withColumn("original_price", F.col("original_price").cast("decimal(18,2)"))
    .withColumn("price_usd", F.round(F.col("price") * F.col("usd_per_unit"), 2).cast("decimal(18,2)"))
    .withColumn(
        "discount_pct",
        F.when(
            F.col("original_price") > F.col("price"),
            F.round(100 * (F.col("original_price") - F.col("price")) / F.col("original_price"), 2),
        ).cast("decimal(6,2)"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicar con `row_number()`

# COMMAND ----------

w = Window.partitionBy("item_id").orderBy(F.col("record_seq").desc())
deduped = valid.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")

snapshot_cols = [
    "item_id",
    "snapshot_date",
    "site_id",
    "search_term",
    "title",
    "category_id",
    "seller_id",
    "condition",
    "listing_type_id",
    "brand",
    "currency_id",
    "price",
    "original_price",
    "usd_per_unit",
    "price_usd",
    "discount_pct",
    "available_quantity",
    "free_shipping",
    "logistic_type",
    "official_store_id",
    "permalink",
]
(
    deduped.select(*snapshot_cols)
    .withColumn("_run_id", F.lit(p.run_id))
    .withColumn("_loaded_at", F.current_timestamp())
    .createOrReplaceTempView("stg_items")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MERGE en Delta (upsert + sincronización de la partición)

# COMMAND ----------

merge_result = spark.sql(f"""
    MERGE INTO {p.table("silver", "item_snapshots")} AS t
    USING stg_items AS s
    ON t.item_id = s.item_id AND t.snapshot_date = s.snapshot_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE
         AND t.snapshot_date = {p.d} AND t.site_id IN ({p.sites_sql}) THEN DELETE
""")
display(merge_result)

# Atributos del vendedor vistos hoy (fuente del SCD2 de Gold): uno por vendedor.
deduped.select(
    "seller_id", "snapshot_date", "site_id", "seller_nickname", "official_store_id", "record_seq"
).createOrReplaceTempView("stg_sellers")

spark.sql(f"""
    INSERT INTO {p.table("silver", "seller_daily")}
    REPLACE WHERE snapshot_date = {p.d} AND site_id IN ({p.sites_sql})
    SELECT seller_id, snapshot_date, site_id, seller_nickname,
           official_store_id IS NOT NULL AS is_official_store, '{p.run_id}' AS _run_id
    FROM (
        SELECT *, row_number() OVER (PARTITION BY seller_id ORDER BY record_seq DESC) AS rn
        FROM stg_sellers
    )
    WHERE rn = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen

# COMMAND ----------

summary = spark.sql(f"""
    SELECT b.site_id,
           b.rows_in,
           coalesce(q.rejected, 0) AS rejected,
           s.rows_out,
           b.rows_in - coalesce(q.rejected, 0) - s.rows_out AS duplicates_removed
    FROM (SELECT site_id, count(*) AS rows_in FROM {p.table("bronze", "ml_items_raw")}
          WHERE ingestion_date = {p.d} AND site_id IN ({p.sites_sql}) GROUP BY site_id) b
    LEFT JOIN (SELECT site_id, count(*) AS rejected FROM {p.table("silver", "items_quarantine")}
               WHERE snapshot_date = {p.d} GROUP BY site_id) q USING (site_id)
    LEFT JOIN (SELECT site_id, count(*) AS rows_out FROM {p.table("silver", "item_snapshots")}
               WHERE snapshot_date = {p.d} GROUP BY site_id) s USING (site_id)
    ORDER BY site_id
""")
display(summary)

for r in summary.collect():
    log_step(
        spark,
        p,
        "silver_build",
        site_id=r.site_id,
        rows_in=r.rows_in,
        rows_out=r.rows_out,
        details={"rejected": r.rejected, "duplicates_removed": r.duplicates_removed},
    )
print("Silver OK")
