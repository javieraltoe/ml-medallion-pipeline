-- Upsert idempotente de una tasa de cambio fechada.
MERGE INTO silver.fx_rates AS t
USING (SELECT %(d)s::date AS rate_date, %(c)s::text AS currency_id,
              %(r)s::numeric AS usd_per_unit, %(src)s::text AS source, %(run)s::text AS run_id) AS s
ON t.rate_date = s.rate_date AND t.currency_id = s.currency_id
WHEN MATCHED THEN
    UPDATE SET usd_per_unit = s.usd_per_unit, source = s.source, _run_id = s.run_id, _loaded_at = now()
WHEN NOT MATCHED THEN
    INSERT (rate_date, currency_id, usd_per_unit, source, _run_id)
    VALUES (s.rate_date, s.currency_id, s.usd_per_unit, s.source, s.run_id);
