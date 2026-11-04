-- Migración de versiones anteriores del proyecto.
-- v1.0 tenía gold.dim_seller SIN historial (seller_id como PK). Si se detecta,
-- se eliminan las tablas de Gold afectadas para recrearlas con el esquema SCD2
-- (001_schemas.sql). Gold se reconstruye desde Silver re-ejecutando el DAG.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'gold' AND table_name = 'dim_seller')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_schema = 'gold' AND table_name = 'dim_seller'
                         AND column_name = 'seller_sk') THEN
        RAISE NOTICE 'Migrando gold.dim_seller a SCD tipo 2: se reconstruye Gold';
        DROP TABLE IF EXISTS gold.mart_category_daily;
        DROP TABLE IF EXISTS gold.fact_item_daily CASCADE;
        DROP TABLE IF EXISTS gold.dim_seller CASCADE;
    END IF;
END $$;
