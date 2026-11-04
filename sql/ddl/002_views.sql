-- Vistas de consumo. CREATE OR REPLACE => idempotente.

-- Último precio conocido por publicación (mismo patrón que "última venta por cliente":
-- ROW_NUMBER + PARTITION BY + ORDER BY fecha DESC con desempate determinista).
CREATE OR REPLACE VIEW gold.vw_latest_item_price AS
WITH ranked AS (
    SELECT f.*,
           ROW_NUMBER() OVER (
               PARTITION BY f.item_id
               ORDER BY f.date_key DESC, f._run_id DESC
           ) AS rn
    FROM gold.fact_item_daily f
)
SELECT date_key, item_id, site_id, category_id, seller_id,
       currency_id, price_local, price_usd, discount_pct
FROM ranked
WHERE rn = 1;

-- Variación de precio día contra día (funciones de ventana LAG).
CREATE OR REPLACE VIEW gold.vw_price_changes AS
SELECT d.full_date,
       f.item_id,
       f.site_id,
       f.category_id,
       f.price_usd,
       LAG(f.price_usd) OVER w                                AS prev_price_usd,
       ROUND(100.0 * (f.price_usd - LAG(f.price_usd) OVER w)
             / NULLIF(LAG(f.price_usd) OVER w, 0), 2)         AS pct_change
FROM gold.fact_item_daily f
JOIN gold.dim_date d USING (date_key)
WINDOW w AS (PARTITION BY f.item_id ORDER BY f.date_key);

-- Resumen del último resultado de cada control de calidad.
CREATE OR REPLACE VIEW audit.vw_dq_latest AS
SELECT DISTINCT ON (run_date, layer, check_name)
       run_date, layer, check_name, dimension, severity, passed, observed, threshold, message, checked_at
FROM audit.dq_results
ORDER BY run_date, layer, check_name, checked_at DESC;
