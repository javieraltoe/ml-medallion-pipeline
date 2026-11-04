-- Consultas para mostrar en una entrevista (abrir con SQLTools en VS Code o `make psql`).
--docker compose exec warehouse psql -U warehouse -d warehouse

-- 1) Precio mediano en USD por país y categoría (último día cargado)
SELECT full_date, country, category_name, items, median_price_usd, pct_free_shipping
FROM gold.mart_category_daily
WHERE full_date = (SELECT max(full_date) FROM gold.mart_category_daily)
ORDER BY category_name, country;

-- 2) Mismo producto, distintos países: ¿dónde es más caro un notebook en USD?
SELECT country, round(avg(median_price_usd), 2) AS mediana_usd_promedio
FROM gold.mart_category_daily
WHERE category_id LIKE '%1652'
GROUP BY country
ORDER BY 2 DESC;

-- 3) Último precio por publicación con ROW_NUMBER (ejercicio clásico de entrevista)
WITH ranked AS (
    SELECT item_id, snapshot_date, price, currency_id, price_usd,
           ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY snapshot_date DESC, _loaded_at DESC) AS rn
    FROM silver.item_snapshots
)
SELECT * FROM ranked WHERE rn = 1 LIMIT 20;

-- 4) Mayores bajas de precio de ayer a hoy (LAG)
SELECT * FROM gold.vw_price_changes
WHERE full_date = (SELECT max(full_date) FROM gold.dim_date)
ORDER BY pct_change ASC NULLS LAST
LIMIT 10;

-- 5) Detectar duplicados en Bronze (lo que Silver resolvió)
SELECT ingestion_date, site_id, item_id, count(*) AS veces
FROM bronze.ml_items_raw
GROUP BY ingestion_date, site_id, item_id
HAVING count(*) > 1
ORDER BY veces DESC
LIMIT 10;

-- 6) ¿Por qué se rechazaron registros?
SELECT snapshot_date, site_id, reason, count(*)
FROM silver.items_quarantine
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;

-- 7) Estado de los controles de calidad del último día
SELECT layer, check_name, dimension, severity, passed, observed, threshold
FROM audit.vw_dq_latest
WHERE run_date = (SELECT max(run_date) FROM audit.dq_results)
ORDER BY layer, check_name;

-- 8) Linaje a nivel registro: de un hecho en Gold hasta el archivo crudo
SELECT f.item_id, f.date_key, f._run_id AS gold_run, s._run_id AS silver_run, b.source_file, b.payload_hash
FROM gold.fact_item_daily f
JOIN silver.item_snapshots s
  ON s.item_id = f.item_id AND to_char(s.snapshot_date, 'YYYYMMDD')::int = f.date_key
JOIN bronze.ml_items_raw b
  ON b.item_id = f.item_id AND b.ingestion_date = s.snapshot_date
LIMIT 5;

-- 9) Volumen diario por país (para ver la serie que usa el control de volumen)
SELECT snapshot_date, site_id, count(*) FROM silver.item_snapshots GROUP BY 1, 2 ORDER BY 1 DESC, 2;
