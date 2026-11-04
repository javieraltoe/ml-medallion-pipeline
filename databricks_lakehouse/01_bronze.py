# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: aterrizar los datos crudos
# MAGIC
# MAGIC 1. Genera las publicaciones del día con la **misma fuente sintética del proyecto local**
# MAGIC    (`src/ml_pipeline/extract/sample_source.py`). En Free Edition el acceso a internet está
# MAGIC    restringido, así que no se llama a la API de Mercado Libre.
# MAGIC 2. Escribe un archivo JSONL por país en el **volumen** de Unity Catalog (zona de aterrizaje).
# MAGIC 3. Lee esos archivos con Spark y los carga en `ml_bronze.ml_items_raw` con
# MAGIC    `INSERT INTO ... REPLACE WHERE` → reemplaza la partición del día, nunca duplica.
# MAGIC 4. Valida el **contrato de esquema** después de guardar el crudo.

# COMMAND ----------

import os
import sys

NB_DIR = os.getcwd()
for path in (NB_DIR, os.path.join(os.path.dirname(NB_DIR), "src")):
    if path not in sys.path:
        sys.path.insert(0, path)

import json

from pyspark.sql import functions as F

from dbx_common import MAX_ITEMS_PER_TERM, SEARCH_TERMS, SITES, get_params, log_step
from ml_pipeline.extract import sample_source
from ml_pipeline.quality import contracts

p = get_params(dbutils)
print(p)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-2. Extraer y escribir en el volumen

# COMMAND ----------

contract = contracts.load_contract("ml_search_item")
reports = {}
files = []

for site in p.sites:
    folder = f"{p.landing_root}/search_items/site_id={site}/ingestion_date={p.run_date.isoformat()}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/part-000.jsonl"
    items = []
    with open(path, "w", encoding="utf-8") as fh:  # sobrescribir = idempotente
        for term in SEARCH_TERMS:
            for item in sample_source.search(site, term, MAX_ITEMS_PER_TERM, p.run_date):
                fh.write(
                    json.dumps(
                        {"search_term": term, "record_seq": len(items), "item": item}, ensure_ascii=False
                    )
                    + "\n"
                )
                items.append(item)
    reports[site] = contracts.validate_records(items, contract)
    files.append(path)
    print(f"{site}: {len(items)} registros -> {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cargar la tabla Bronze (reemplazo de la partición del día)

# COMMAND ----------

raw = (
    spark.read.text(files)
    .select(
        F.col("value"),
        F.col("_metadata.file_path").alias("source_file"),
    )
    .select(
        F.lit(p.run_date).cast("date").alias("ingestion_date"),
        F.regexp_extract("source_file", r"site_id=([A-Z]+)", 1).alias("site_id"),
        F.get_json_object("value", "$.search_term").alias("search_term"),
        F.get_json_object("value", "$.record_seq").cast("int").alias("record_seq"),
        F.get_json_object("value", "$.item.id").alias("item_id"),
        F.get_json_object("value", "$.item").alias("payload"),
        "source_file",
    )
    .withColumn("payload_hash", F.sha2("payload", 256))
    .withColumn("source_mode", F.lit("sample"))
    .withColumn("run_id", F.lit(p.run_id))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "ingestion_date",
        "site_id",
        "search_term",
        "record_seq",
        "item_id",
        "payload",
        "payload_hash",
        "source_mode",
        "source_file",
        "run_id",
        "ingested_at",
    )
)
raw.createOrReplaceTempView("bronze_batch")

spark.sql(f"""
    INSERT INTO {p.table("bronze", "ml_items_raw")}
    REPLACE WHERE ingestion_date = {p.d} AND site_id IN ({p.sites_sql})
    SELECT * FROM bronze_batch
""")

counts = {
    r.site_id: r.n
    for r in spark.sql(f"""
    SELECT site_id, count(*) AS n FROM {p.table("bronze", "ml_items_raw")}
    WHERE ingestion_date = {p.d} GROUP BY site_id
""").collect()
}
print(counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasas de cambio del día (Bronze crudo + Silver con MERGE)

# COMMAND ----------

currencies = sorted({SITES[s]["currency"] for s in p.sites} | {"USD"})
fx_rows = [
    (
        p.run_date,
        c,
        json.dumps({"from": c, "to": "USD", "ratio": sample_source.usd_rate(c, p.run_date)}),
        "sample",
        p.run_id,
    )
    for c in currencies
]
(
    spark.createDataFrame(
        fx_rows, "rate_date date, currency_id string, payload string, source_mode string, run_id string"
    )
    .withColumn("ingested_at", F.current_timestamp())
    .createOrReplaceTempView("fx_batch")
)

spark.sql(f"""
    INSERT INTO {p.table("bronze", "fx_rates_raw")}
    REPLACE WHERE rate_date = {p.d}
    SELECT rate_date, currency_id, payload, source_mode, run_id, ingested_at FROM fx_batch
""")

spark.sql(f"""
    MERGE INTO {p.table("silver", "fx_rates")} AS t
    USING (
        SELECT rate_date, currency_id,
               CAST(get_json_object(payload, '$.ratio') AS DECIMAL(20,10)) AS usd_per_unit,
               source_mode AS source, run_id
        FROM fx_batch
    ) AS s
    ON t.rate_date = s.rate_date AND t.currency_id = s.currency_id
    WHEN MATCHED THEN UPDATE SET usd_per_unit = s.usd_per_unit, source = s.source,
                                 _run_id = s.run_id, _loaded_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (rate_date, currency_id, usd_per_unit, source, _run_id, _loaded_at)
                          VALUES (s.rate_date, s.currency_id, s.usd_per_unit, s.source, s.run_id, current_timestamp())
""")
display(spark.table(p.table("silver", "fx_rates")).where(f"rate_date = {p.d}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Contrato de esquema (después de guardar el crudo)

# COMMAND ----------

for site in p.sites:
    log_step(
        spark,
        p,
        "bronze_extract",
        site_id=site,
        rows_in=counts.get(site, 0),
        rows_out=counts.get(site, 0),
        details={"contract": reports[site].as_dict()},
    )

for site, report in reports.items():
    print(site, report.as_dict())
    contracts.enforce(report, contract["max_violation_ratio"])  # falla la tarea si se rompe el contrato

print("Bronze OK")
