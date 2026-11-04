# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gobierno de datos con Unity Catalog
# MAGIC
# MAGIC Unity Catalog centraliza en un solo lugar **permisos, documentación, descubrimiento,
# MAGIC auditoría y linaje** de todas las tablas. Este notebook aplica sobre el lakehouse:
# MAGIC
# MAGIC 1. Documentación: comentarios en tablas y columnas.
# MAGIC 2. Etiquetas (*tags*) para clasificar por capa y sensibilidad.
# MAGIC 3. Permisos: lectura de Gold para todos los usuarios, nada más.
# MAGIC 4. Vista dinámica que enmascara datos sensibles según quién consulta.
# MAGIC 5. Cómo ver el **linaje** automático.
# MAGIC
# MAGIC Se ejecuta una vez (o cuando cambie el modelo); no es parte del job diario.

# COMMAND ----------

import os
import sys

NB_DIR = os.getcwd()
if NB_DIR not in sys.path:
    sys.path.insert(0, NB_DIR)

from dbx_common import get_params

p = get_params(dbutils)
T = p.table


def run(sql: str, optional: bool = False) -> None:
    """Ejecuta y reporta. Los pasos opcionales no frenan el notebook si la edición no los soporta."""
    try:
        spark.sql(sql)
        print(f"OK   {sql.strip().splitlines()[0][:110]}")
    except Exception as exc:  # noqa: BLE001
        if not optional:
            raise
        print(f"SKIP {sql.strip().splitlines()[0][:80]} -> {str(exc).splitlines()[0][:120]}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Documentación en el catálogo

# COMMAND ----------

run(
    f"COMMENT ON TABLE {T('gold', 'fact_item_daily')} IS "
    "'Hechos de publicaciones de Mercado Libre. Grano: publicación x día. Precios en moneda local y USD.'"
)
run(
    f"COMMENT ON TABLE {T('gold', 'mart_category_daily')} IS "
    "'Precio mediano en USD, percentiles, envío gratis y descuento por país, categoría y día. Listo para BI.'"
)

column_docs = {
    ("gold", "fact_item_daily"): {
        "seller_sk": "Versión del vendedor vigente el día del hecho (SCD2)",
        "price_local": "Precio en la moneda original de la publicación",
        "price_usd": "Precio convertido a USD con la tasa del día",
        "discount_pct": "Descuento sobre el precio original, si existe",
    },
    ("gold", "dim_seller"): {
        "valid_from": "Inicio de vigencia de la versión (inclusive)",
        "valid_to": "Fin de vigencia (exclusivo). 9999-12-31 = vigente",
        "row_hash": "Hash de los atributos para detectar cambios",
    },
    ("silver", "item_snapshots"): {
        "usd_per_unit": "Tasa de cambio aplicada (trazabilidad de la conversión)",
    },
}
for (layer, table), cols in column_docs.items():
    for col, text in cols.items():
        run(f"ALTER TABLE {T(layer, table)} ALTER COLUMN {col} COMMENT '{text}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Etiquetas (tags)

# COMMAND ----------

for layer in ("bronze", "silver", "gold"):
    run(
        f"ALTER SCHEMA {p.schema(layer)} SET TAGS ('capa' = '{layer}', 'proyecto' = 'ml-medallion')",
        optional=True,
    )

run(
    f"ALTER TABLE {T('gold', 'dim_seller')} SET TAGS ('contiene_pii' = 'baja', 'scd' = 'tipo2')",
    optional=True,
)
run(
    f"ALTER TABLE {T('gold', 'dim_seller')} ALTER COLUMN nickname SET TAGS ('sensible' = 'si')", optional=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Permisos
# MAGIC
# MAGIC Principio de mínimo privilegio: todos los usuarios pueden **leer Gold**; Bronze y Silver
# MAGIC quedan sólo para el dueño del pipeline. `account users` es el grupo que incluye a todos.

# COMMAND ----------

run(f"GRANT USE CATALOG ON CATALOG {p.catalog} TO `account users`", optional=True)
run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {p.schema('gold')} TO `account users`", optional=True)
display(spark.sql(f"SHOW GRANTS ON SCHEMA {p.schema('gold')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Vista dinámica: enmascarar datos sensibles
# MAGIC
# MAGIC Patrón clásico de Unity Catalog: en lugar de dar acceso a la tabla, se publica una vista que
# MAGIC enmascara la columna según quién consulta. Quien no pertenezca al grupo `admins` del workspace
# MAGIC ve el nombre del vendedor parcialmente oculto. La tabla original no se toca, así que el job diario
# MAGIC sigue funcionando igual.

# COMMAND ----------

run(
    f"""
CREATE OR REPLACE VIEW {p.schema("gold")}.vw_seller_public
COMMENT 'Vendedores vigentes con el nombre enmascarado para quien no es admin'
AS SELECT seller_id, site_id,
          CASE WHEN is_member('admins') THEN nickname
               ELSE concat(substr(nickname, 1, 3), '***') END AS nickname,
          is_official_store, valid_from
   FROM {T("gold", "dim_seller")}
   WHERE is_current
""",
    optional=True,
)
if spark.catalog.tableExists(f"{p.schema('gold')}.vw_seller_public"):
    display(spark.sql(f"SELECT * FROM {p.schema('gold')}.vw_seller_public LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Linaje automático
# MAGIC
# MAGIC Unity Catalog registra el linaje de cada consulta sin configurar nada:
# MAGIC
# MAGIC 1. Menú **Catalog** → `workspace` → `ml_gold` → `mart_category_daily`.
# MAGIC 2. Pestaña **Lineage** → **See lineage graph**.
# MAGIC 3. Se ve el recorrido `ml_items_raw` → `item_snapshots` → `fact_item_daily` → `mart_category_daily`,
# MAGIC    y también a nivel de columna (por ejemplo, de dónde sale `price_usd`).
# MAGIC
# MAGIC Las mismas consultas también quedan en las tablas de sistema (`system.access.table_lineage`)
# MAGIC si la edición las tiene habilitadas.

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {T('gold', 'fact_item_daily')}"))
