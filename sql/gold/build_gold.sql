-- ==========================================================================
-- GOLD: modelo estrella + data mart. Patrón "delete + insert por fecha":
-- re-ejecutar una fecha la reconstruye entera sin duplicar.
-- Parámetros: %(d)s (fecha del snapshot), %(run_id)s
-- ==========================================================================

-- Dimensión categoría: los ids que no estén en el seed quedan identificados.
INSERT INTO gold.dim_category (category_id, site_id, category_name)
SELECT DISTINCT s.category_id, s.site_id, 'Sin clasificar (' || s.category_id || ')'
FROM silver.item_snapshots s
WHERE s.snapshot_date = %(d)s
ON CONFLICT (category_id) DO NOTHING;

-- ==========================================================================
-- Dimensión vendedor: SCD TIPO 2 (patrón "expirar + insertar")
-- Se procesa un día a la vez, en orden (max_active_runs=1 en el DAG).
-- ==========================================================================
CREATE TEMP TABLE stg_seller ON COMMIT DROP AS
SELECT seller_id, site_id, nickname, is_official_store,
       md5(concat_ws('|', nickname, is_official_store::text)) AS row_hash
FROM silver.seller_daily
WHERE snapshot_date = %(d)s;

-- a) Re-ejecución del mismo día con datos distintos: se corrige la versión de ese día.
UPDATE gold.dim_seller t
SET nickname = s.nickname, is_official_store = s.is_official_store, row_hash = s.row_hash
FROM stg_seller s
WHERE t.seller_id = s.seller_id AND t.is_current
  AND t.valid_from = %(d)s AND t.row_hash <> s.row_hash;

-- b) Cambió algún atributo: se cierra la versión vigente.
UPDATE gold.dim_seller t
SET valid_to = %(d)s, is_current = FALSE
FROM stg_seller s
WHERE t.seller_id = s.seller_id AND t.is_current
  AND t.valid_from < %(d)s AND t.row_hash <> s.row_hash;

-- c) Vendedores nuevos o recién cerrados: se abre una versión vigente.
INSERT INTO gold.dim_seller (seller_id, site_id, nickname, is_official_store, row_hash, valid_from)
SELECT s.seller_id, s.site_id, s.nickname, s.is_official_store, s.row_hash, %(d)s
FROM stg_seller s
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_seller t WHERE t.seller_id = s.seller_id AND t.is_current);

-- Dimensión fecha
INSERT INTO gold.dim_date (date_key, full_date, year, quarter, month, day, day_of_week, is_weekend)
SELECT TO_CHAR(d, 'YYYYMMDD')::int, d::date,
       EXTRACT(YEAR FROM d), EXTRACT(QUARTER FROM d), EXTRACT(MONTH FROM d), EXTRACT(DAY FROM d),
       EXTRACT(ISODOW FROM d), EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(%(d)s::date, %(d)s::date, interval '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- Hechos
DELETE FROM gold.fact_item_daily WHERE date_key = TO_CHAR(%(d)s::date, 'YYYYMMDD')::int;

-- Cada hecho apunta a la versión del vendedor VIGENTE ESE DÍA (point-in-time).
-- Si se hace backfill de una fecha anterior a la primera versión conocida, se usa la más antigua.
INSERT INTO gold.fact_item_daily (
    date_key, item_id, site_id, category_id, seller_sk, seller_id, currency_id, price_local,
    price_usd, discount_pct, available_quantity, free_shipping, is_new, _run_id
)
SELECT TO_CHAR(s.snapshot_date, 'YYYYMMDD')::int, s.item_id, s.site_id, s.category_id, v.seller_sk,
       s.seller_id, s.currency_id, s.price, s.price_usd, s.discount_pct, s.available_quantity,
       s.free_shipping, s.condition = 'new', %(run_id)s
FROM silver.item_snapshots s
CROSS JOIN LATERAL (
    SELECT d.seller_sk
    FROM gold.dim_seller d
    WHERE d.seller_id = s.seller_id
    ORDER BY (d.valid_from <= s.snapshot_date AND s.snapshot_date < d.valid_to) DESC, d.valid_from
    LIMIT 1
) v
WHERE s.snapshot_date = %(d)s;

-- Data mart por categoría
DELETE FROM gold.mart_category_daily WHERE full_date = %(d)s;

INSERT INTO gold.mart_category_daily (
    full_date, site_id, country, category_id, category_name, items, sellers, avg_price_usd,
    median_price_usd, p25_price_usd, p75_price_usd, pct_free_shipping, avg_discount_pct, _run_id
)
SELECT d.full_date, f.site_id, st.country, f.category_id, c.category_name,
       COUNT(*), COUNT(DISTINCT f.seller_id),
       ROUND(AVG(f.price_usd), 2),
       ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.price_usd)::numeric, 2),
       ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.price_usd)::numeric, 2),
       ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.price_usd)::numeric, 2),
       ROUND(100.0 * AVG(CASE WHEN f.free_shipping THEN 1 ELSE 0 END), 2),
       ROUND(AVG(f.discount_pct), 2),
       %(run_id)s
FROM gold.fact_item_daily f
JOIN gold.dim_date d     ON d.date_key = f.date_key
JOIN gold.dim_site st    ON st.site_id = f.site_id
JOIN gold.dim_category c ON c.category_id = f.category_id
WHERE d.full_date = %(d)s
GROUP BY d.full_date, f.site_id, st.country, f.category_id, c.category_name;
