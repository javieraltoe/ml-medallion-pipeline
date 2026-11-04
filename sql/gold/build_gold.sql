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

-- Dimensión vendedor (SCD1)
INSERT INTO gold.dim_seller (seller_id, site_id, nickname, is_official_store, first_seen_date)
SELECT seller_id, site_id, nickname, official_store_id IS NOT NULL, first_seen_date
FROM silver.sellers
ON CONFLICT (seller_id) DO UPDATE SET
    nickname = EXCLUDED.nickname,
    is_official_store = EXCLUDED.is_official_store,
    first_seen_date = LEAST(gold.dim_seller.first_seen_date, EXCLUDED.first_seen_date);

-- Dimensión fecha
INSERT INTO gold.dim_date (date_key, full_date, year, quarter, month, day, day_of_week, is_weekend)
SELECT TO_CHAR(d, 'YYYYMMDD')::int, d::date,
       EXTRACT(YEAR FROM d), EXTRACT(QUARTER FROM d), EXTRACT(MONTH FROM d), EXTRACT(DAY FROM d),
       EXTRACT(ISODOW FROM d), EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(%(d)s::date, %(d)s::date, interval '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- Hechos
DELETE FROM gold.fact_item_daily WHERE date_key = TO_CHAR(%(d)s::date, 'YYYYMMDD')::int;

INSERT INTO gold.fact_item_daily (
    date_key, item_id, site_id, category_id, seller_id, currency_id, price_local, price_usd,
    discount_pct, available_quantity, free_shipping, is_new, _run_id
)
SELECT TO_CHAR(s.snapshot_date, 'YYYYMMDD')::int, s.item_id, s.site_id, s.category_id, s.seller_id,
       s.currency_id, s.price, s.price_usd, s.discount_pct, s.available_quantity, s.free_shipping,
       s.condition = 'new', %(run_id)s
FROM silver.item_snapshots s
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
