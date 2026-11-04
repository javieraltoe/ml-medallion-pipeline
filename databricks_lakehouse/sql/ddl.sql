-- Tablas Delta en Unity Catalog. {catalog} se reemplaza desde el notebook 00_setup.
-- CREATE ... IF NOT EXISTS: se puede ejecutar N veces.
-- CLUSTER BY = liquid clustering (reemplaza al particionado clásico en Delta).

CREATE SCHEMA IF NOT EXISTS {catalog}.ml_bronze COMMENT 'Capa Bronze: datos crudos tal cual llegan';
CREATE SCHEMA IF NOT EXISTS {catalog}.ml_silver COMMENT 'Capa Silver: datos limpios, deduplicados y normalizados a USD';
CREATE SCHEMA IF NOT EXISTS {catalog}.ml_gold   COMMENT 'Capa Gold: modelo estrella y data marts';
CREATE SCHEMA IF NOT EXISTS {catalog}.ml_audit  COMMENT 'Auditoría: ejecuciones y controles de calidad';

CREATE VOLUME IF NOT EXISTS {catalog}.ml_bronze.landing
COMMENT 'Zona de aterrizaje: archivos JSONL crudos particionados por site_id / ingestion_date';

-- ======================================================================= BRONZE
CREATE TABLE IF NOT EXISTS {catalog}.ml_bronze.ml_items_raw (
    ingestion_date DATE      NOT NULL,
    site_id        STRING    NOT NULL,
    search_term    STRING,
    record_seq     INT,
    item_id        STRING,
    payload        STRING    NOT NULL COMMENT 'JSON crudo de la publicación',
    payload_hash   STRING    NOT NULL,
    source_mode    STRING    NOT NULL,
    source_file    STRING    NOT NULL COMMENT 'Archivo del volumen de origen (linaje)',
    run_id         STRING    NOT NULL,
    ingested_at    TIMESTAMP NOT NULL
) CLUSTER BY (ingestion_date, site_id)
COMMENT 'Publicaciones crudas de /sites/{site}/search. Conserva duplicados y errores.';

CREATE TABLE IF NOT EXISTS {catalog}.ml_bronze.fx_rates_raw (
    rate_date    DATE      NOT NULL,
    currency_id  STRING    NOT NULL,
    payload      STRING    NOT NULL,
    source_mode  STRING    NOT NULL,
    run_id       STRING    NOT NULL,
    ingested_at  TIMESTAMP NOT NULL
);

-- ======================================================================= SILVER
CREATE TABLE IF NOT EXISTS {catalog}.ml_silver.fx_rates (
    rate_date     DATE           NOT NULL,
    currency_id   STRING         NOT NULL,
    usd_per_unit  DECIMAL(20,10) NOT NULL,
    source        STRING         NOT NULL,
    _run_id       STRING         NOT NULL,
    _loaded_at    TIMESTAMP      NOT NULL
) COMMENT 'Tasa de cambio diaria: USD por unidad de moneda';

CREATE TABLE IF NOT EXISTS {catalog}.ml_silver.item_snapshots (
    item_id             STRING         NOT NULL,
    snapshot_date       DATE           NOT NULL,
    site_id             STRING         NOT NULL,
    search_term         STRING,
    title               STRING         NOT NULL,
    category_id         STRING         NOT NULL,
    seller_id           BIGINT         NOT NULL,
    condition           STRING,
    listing_type_id     STRING,
    brand               STRING,
    currency_id         STRING         NOT NULL COMMENT 'Moneda original',
    price               DECIMAL(18,2)  NOT NULL COMMENT 'Precio en moneda original',
    original_price      DECIMAL(18,2),
    usd_per_unit        DECIMAL(20,10) NOT NULL COMMENT 'Tasa aplicada',
    price_usd           DECIMAL(18,2)  NOT NULL,
    discount_pct        DECIMAL(6,2),
    available_quantity  INT,
    free_shipping       BOOLEAN,
    logistic_type       STRING,
    official_store_id   BIGINT,
    permalink           STRING,
    _run_id             STRING         NOT NULL,
    _loaded_at          TIMESTAMP      NOT NULL
) CLUSTER BY (snapshot_date, site_id)
COMMENT 'Una fila por publicación y día. Clave: (item_id, snapshot_date).';

CREATE TABLE IF NOT EXISTS {catalog}.ml_silver.items_quarantine (
    snapshot_date  DATE      NOT NULL,
    site_id        STRING    NOT NULL,
    item_id        STRING,
    reason         STRING    NOT NULL,
    payload        STRING    NOT NULL,
    _run_id        STRING    NOT NULL,
    quarantined_at TIMESTAMP NOT NULL
) COMMENT 'Registros que no cumplen las reglas de negocio, con el motivo';

CREATE TABLE IF NOT EXISTS {catalog}.ml_silver.seller_daily (
    seller_id          BIGINT  NOT NULL,
    snapshot_date      DATE    NOT NULL,
    site_id            STRING  NOT NULL,
    nickname           STRING,
    is_official_store  BOOLEAN NOT NULL,
    _run_id            STRING  NOT NULL
) COMMENT 'Atributos del vendedor vistos cada día (fuente del SCD tipo 2)';

-- ======================================================================= GOLD
CREATE TABLE IF NOT EXISTS {catalog}.ml_gold.dim_site (
    site_id       STRING NOT NULL,
    country       STRING NOT NULL,
    country_code  STRING NOT NULL,
    currency_id   STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS {catalog}.ml_gold.dim_category (
    category_id    STRING NOT NULL,
    site_id        STRING NOT NULL,
    category_name  STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS {catalog}.ml_gold.dim_date (
    date_key     INT      NOT NULL,
    full_date    DATE     NOT NULL,
    year         SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,
    is_weekend   BOOLEAN  NOT NULL
);

CREATE TABLE IF NOT EXISTS {catalog}.ml_gold.dim_seller (
    seller_sk          BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Clave subrogada (una por versión)',
    seller_id          BIGINT  NOT NULL COMMENT 'Clave natural',
    site_id            STRING  NOT NULL,
    nickname           STRING,
    is_official_store  BOOLEAN NOT NULL,
    row_hash           STRING  NOT NULL,
    valid_from         DATE    NOT NULL,
    valid_to           DATE    NOT NULL,
    is_current         BOOLEAN NOT NULL
) COMMENT 'Vendedor con historial (SCD tipo 2)';

CREATE TABLE IF NOT EXISTS {catalog}.ml_gold.fact_item_daily (
    date_key            INT           NOT NULL,
    item_id             STRING        NOT NULL,
    site_id             STRING        NOT NULL,
    category_id         STRING        NOT NULL,
    seller_sk           BIGINT        NOT NULL,
    seller_id           BIGINT        NOT NULL,
    currency_id         STRING        NOT NULL,
    price_local         DECIMAL(18,2) NOT NULL,
    price_usd           DECIMAL(18,2) NOT NULL,
    discount_pct        DECIMAL(6,2),
    available_quantity  INT,
    free_shipping       BOOLEAN,
    is_new              BOOLEAN,
    _run_id             STRING        NOT NULL
) CLUSTER BY (date_key)
COMMENT 'Hechos: grano publicación x día';

CREATE TABLE IF NOT EXISTS {catalog}.ml_gold.mart_category_daily (
    full_date          DATE          NOT NULL,
    site_id            STRING        NOT NULL,
    country            STRING        NOT NULL,
    category_id        STRING        NOT NULL,
    category_name      STRING        NOT NULL,
    items              BIGINT        NOT NULL,
    sellers            BIGINT        NOT NULL,
    avg_price_usd      DECIMAL(18,2) NOT NULL,
    median_price_usd   DECIMAL(18,2) NOT NULL,
    p25_price_usd      DECIMAL(18,2) NOT NULL,
    p75_price_usd      DECIMAL(18,2) NOT NULL,
    pct_free_shipping  DECIMAL(5,2)  NOT NULL,
    avg_discount_pct   DECIMAL(6,2),
    _run_id            STRING        NOT NULL
) COMMENT 'Mart por país, categoría y día';

-- ======================================================================= AUDIT
CREATE TABLE IF NOT EXISTS {catalog}.ml_audit.pipeline_runs (
    run_id      STRING    NOT NULL,
    step        STRING    NOT NULL,
    site_id     STRING,
    run_date    DATE      NOT NULL,
    status      STRING    NOT NULL,
    rows_in     BIGINT,
    rows_out    BIGINT,
    details     STRING,
    logged_at   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS {catalog}.ml_audit.dq_results (
    run_id      STRING    NOT NULL,
    run_date    DATE      NOT NULL,
    layer       STRING    NOT NULL,
    check_name  STRING    NOT NULL,
    dimension   STRING    NOT NULL,
    severity    STRING    NOT NULL,
    passed      BOOLEAN   NOT NULL,
    observed    DOUBLE,
    threshold   DOUBLE,
    message     STRING,
    checked_at  TIMESTAMP NOT NULL
);
