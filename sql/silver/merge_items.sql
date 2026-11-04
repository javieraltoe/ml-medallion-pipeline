-- ==========================================================================
-- SILVER: deduplicación + carga incremental idempotente (MERGE)
-- Parámetros: %(snapshot_date)s, %(site_id)s, %(run_id)s
-- ==========================================================================

-- 1) Deduplicar: una fila por publicación. Si la misma publicación aparece
--    varias veces (paginación solapada, varios términos de búsqueda), nos
--    quedamos con la última recibida. Desempate determinista por record_seq.
CREATE TEMP TABLE stg_items ON COMMIT DROP AS
SELECT *
FROM (
    SELECT r.*,
           ROW_NUMBER() OVER (PARTITION BY r.item_id ORDER BY r.record_seq DESC) AS rn
    FROM stg_items_raw r
) d
WHERE rn = 1;

-- 2) Sincronizar la partición: si una re-ejecución trae menos publicaciones,
--    eliminamos las que ya no están (equivale a WHEN NOT MATCHED BY SOURCE de PG17).
DELETE FROM silver.item_snapshots t
WHERE t.snapshot_date = %(snapshot_date)s
  AND t.site_id = %(site_id)s
  AND NOT EXISTS (SELECT 1 FROM stg_items s WHERE s.item_id = t.item_id);

-- 3) Histórico diario (grano: publicación x día)
MERGE INTO silver.item_snapshots AS t
USING stg_items AS s
ON t.item_id = s.item_id AND t.snapshot_date = s.snapshot_date
WHEN MATCHED THEN UPDATE SET
    site_id = s.site_id, search_term = s.search_term, title = s.title, category_id = s.category_id,
    seller_id = s.seller_id, condition = s.condition, listing_type_id = s.listing_type_id,
    brand = s.brand, currency_id = s.currency_id, price = s.price, original_price = s.original_price,
    usd_per_unit = s.usd_per_unit, price_usd = s.price_usd, discount_pct = s.discount_pct,
    available_quantity = s.available_quantity, free_shipping = s.free_shipping,
    logistic_type = s.logistic_type, official_store_id = s.official_store_id,
    permalink = s.permalink, _run_id = %(run_id)s, _loaded_at = now()
WHEN NOT MATCHED THEN INSERT (
    item_id, snapshot_date, site_id, search_term, title, category_id, seller_id, condition,
    listing_type_id, brand, currency_id, price, original_price, usd_per_unit, price_usd,
    discount_pct, available_quantity, free_shipping, logistic_type, official_store_id,
    permalink, _run_id
) VALUES (
    s.item_id, s.snapshot_date, s.site_id, s.search_term, s.title, s.category_id, s.seller_id,
    s.condition, s.listing_type_id, s.brand, s.currency_id, s.price, s.original_price,
    s.usd_per_unit, s.price_usd, s.discount_pct, s.available_quantity, s.free_shipping,
    s.logistic_type, s.official_store_id, s.permalink, %(run_id)s
);

-- 4) Estado actual (SCD1). Un backfill de una fecha vieja NO pisa datos más nuevos.
MERGE INTO silver.items AS t
USING stg_items AS s
ON t.item_id = s.item_id
WHEN MATCHED AND s.snapshot_date >= t.last_seen_date THEN UPDATE SET
    title = s.title, category_id = s.category_id, seller_id = s.seller_id, brand = s.brand,
    condition = s.condition, currency_id = s.currency_id, last_price = s.price,
    last_price_usd = s.price_usd, last_seen_date = s.snapshot_date,
    first_seen_date = LEAST(t.first_seen_date, s.snapshot_date),
    _run_id = %(run_id)s, _updated_at = now()
WHEN MATCHED THEN UPDATE SET
    first_seen_date = LEAST(t.first_seen_date, s.snapshot_date)
WHEN NOT MATCHED THEN INSERT (
    item_id, site_id, title, category_id, seller_id, brand, condition, currency_id,
    last_price, last_price_usd, first_seen_date, last_seen_date, _run_id
) VALUES (
    s.item_id, s.site_id, s.title, s.category_id, s.seller_id, s.brand, s.condition,
    s.currency_id, s.price, s.price_usd, s.snapshot_date, s.snapshot_date, %(run_id)s
);

-- 5) Vendedores
MERGE INTO silver.sellers AS t
USING (
    SELECT DISTINCT ON (seller_id) seller_id, site_id, seller_nickname, official_store_id, snapshot_date
    FROM stg_items
    ORDER BY seller_id, record_seq DESC
) AS s
ON t.seller_id = s.seller_id
WHEN MATCHED THEN UPDATE SET
    nickname = CASE WHEN s.snapshot_date >= t.last_seen_date THEN s.seller_nickname ELSE t.nickname END,
    official_store_id = COALESCE(s.official_store_id, t.official_store_id),
    first_seen_date = LEAST(t.first_seen_date, s.snapshot_date),
    last_seen_date = GREATEST(t.last_seen_date, s.snapshot_date),
    _updated_at = now()
WHEN NOT MATCHED THEN INSERT (seller_id, site_id, nickname, official_store_id, first_seen_date, last_seen_date)
VALUES (s.seller_id, s.site_id, s.seller_nickname, s.official_store_id, s.snapshot_date, s.snapshot_date);

-- 6) Atributos del vendedor vistos HOY (fuente del SCD tipo 2 en Gold).
--    Delete + insert de la partición (fecha, sitio) => idempotente.
DELETE FROM silver.seller_daily
WHERE snapshot_date = %(snapshot_date)s AND site_id = %(site_id)s;

INSERT INTO silver.seller_daily (seller_id, snapshot_date, site_id, nickname, is_official_store, _run_id)
SELECT DISTINCT ON (seller_id)
       seller_id, snapshot_date, site_id, seller_nickname, official_store_id IS NOT NULL, %(run_id)s
FROM stg_items
ORDER BY seller_id, record_seq DESC;
