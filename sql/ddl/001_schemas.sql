-- Migración idempotente: se puede ejecutar N veces sin efectos secundarios.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================ AUDIT
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT        NOT NULL,
    step          TEXT        NOT NULL,
    site_id       TEXT,
    run_date      DATE        NOT NULL,
    status        TEXT        NOT NULL,            -- success | failed
    rows_in       INTEGER,
    rows_out      INTEGER,
    details       JSONB,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit.dq_results (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT        NOT NULL,
    run_date      DATE        NOT NULL,
    layer         TEXT        NOT NULL,            -- bronze | silver | gold
    check_name    TEXT        NOT NULL,
    dimension     TEXT        NOT NULL,            -- completeness, uniqueness, validity, ...
    severity      TEXT        NOT NULL,            -- error | warn
    passed        BOOLEAN     NOT NULL,
    observed      NUMERIC,
    threshold     NUMERIC,
    message       TEXT,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_results_date ON audit.dq_results (run_date, layer);

-- ============================================================ BRONZE
-- Datos crudos tal cual llegan (JSON). Se conservan duplicados y errores:
-- Bronze es la "evidencia" para reprocesar y auditar.
CREATE TABLE IF NOT EXISTS bronze.ml_items_raw (
    ingestion_date  DATE        NOT NULL,
    site_id         TEXT        NOT NULL,
    search_term     TEXT        NOT NULL,
    record_seq      INTEGER     NOT NULL,          -- orden de llegada dentro del lote
    item_id         TEXT,
    payload         JSONB       NOT NULL,
    payload_hash    TEXT        NOT NULL,
    source_mode     TEXT        NOT NULL,          -- sample | api
    source_file     TEXT        NOT NULL,          -- lineage hacia el data lake
    run_id          TEXT        NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_bronze_items_partition ON bronze.ml_items_raw (ingestion_date, site_id);

CREATE TABLE IF NOT EXISTS bronze.fx_rates_raw (
    rate_date     DATE        NOT NULL,
    currency_id   TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    source_mode   TEXT        NOT NULL,
    run_id        TEXT        NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================ SILVER
CREATE TABLE IF NOT EXISTS silver.fx_rates (
    rate_date     DATE           NOT NULL,
    currency_id   TEXT           NOT NULL,
    usd_per_unit  NUMERIC(20,10) NOT NULL CHECK (usd_per_unit > 0),
    source        TEXT           NOT NULL,
    _run_id       TEXT           NOT NULL,
    _loaded_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, currency_id)
);

-- Una fila por publicación y día (grano del histórico de precios).
CREATE TABLE IF NOT EXISTS silver.item_snapshots (
    item_id             TEXT           NOT NULL,
    snapshot_date       DATE           NOT NULL,
    site_id             TEXT           NOT NULL,
    search_term         TEXT           NOT NULL,
    title               TEXT           NOT NULL,
    category_id         TEXT           NOT NULL,
    seller_id           BIGINT         NOT NULL,
    condition           TEXT,
    listing_type_id     TEXT,
    brand               TEXT,
    currency_id         TEXT           NOT NULL,   -- se conserva la moneda original
    price               NUMERIC(18,2)  NOT NULL,
    original_price      NUMERIC(18,2),
    usd_per_unit        NUMERIC(20,10) NOT NULL,   -- tasa aplicada (trazabilidad)
    price_usd           NUMERIC(18,2)  NOT NULL,
    discount_pct        NUMERIC(6,2),
    available_quantity  INTEGER,
    free_shipping       BOOLEAN,
    logistic_type       TEXT,
    official_store_id   BIGINT,
    permalink           TEXT,
    _run_id             TEXT           NOT NULL,
    _loaded_at          TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (item_id, snapshot_date)
);
CREATE INDEX IF NOT EXISTS ix_snapshots_date_site ON silver.item_snapshots (snapshot_date, site_id);

-- Estado actual de cada publicación (SCD tipo 1).
CREATE TABLE IF NOT EXISTS silver.items (
    item_id           TEXT          PRIMARY KEY,
    site_id           TEXT          NOT NULL,
    title             TEXT          NOT NULL,
    category_id       TEXT          NOT NULL,
    seller_id         BIGINT        NOT NULL,
    brand             TEXT,
    condition         TEXT,
    currency_id       TEXT          NOT NULL,
    last_price        NUMERIC(18,2) NOT NULL,
    last_price_usd    NUMERIC(18,2) NOT NULL,
    first_seen_date   DATE          NOT NULL,
    last_seen_date    DATE          NOT NULL,
    _run_id           TEXT          NOT NULL,
    _updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.sellers (
    seller_id          BIGINT      PRIMARY KEY,
    site_id            TEXT        NOT NULL,
    nickname           TEXT,
    official_store_id  BIGINT,
    first_seen_date    DATE        NOT NULL,
    last_seen_date     DATE        NOT NULL,
    _updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Atributos del vendedor tal como se vieron cada día: es la fuente del SCD2 de Gold.
CREATE TABLE IF NOT EXISTS silver.seller_daily (
    seller_id          BIGINT      NOT NULL,
    snapshot_date      DATE        NOT NULL,
    site_id            TEXT        NOT NULL,
    nickname           TEXT,
    is_official_store  BOOLEAN     NOT NULL,
    _run_id            TEXT        NOT NULL,
    PRIMARY KEY (seller_id, snapshot_date)
);

-- Registros que no pasan las reglas de negocio: no se pierden, se aíslan.
CREATE TABLE IF NOT EXISTS silver.items_quarantine (
    snapshot_date   DATE        NOT NULL,
    site_id         TEXT        NOT NULL,
    item_id         TEXT,
    reason          TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    _run_id         TEXT        NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_quarantine_partition ON silver.items_quarantine (snapshot_date, site_id);

-- ============================================================ GOLD (esquema estrella)
CREATE TABLE IF NOT EXISTS gold.dim_site (
    site_id       TEXT PRIMARY KEY,
    country       TEXT NOT NULL,
    country_code  TEXT NOT NULL,
    currency_id   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_category (
    category_id    TEXT PRIMARY KEY,
    site_id        TEXT NOT NULL REFERENCES gold.dim_site (site_id),
    category_name  TEXT NOT NULL
);

-- Dimensión vendedor con historial (SCD tipo 2): una fila por VERSIÓN del vendedor.
-- Si cambia el nombre o pasa a ser tienda oficial, se cierra la versión vigente
-- (valid_to) y se abre una nueva. Los hechos apuntan a la versión vigente ese día.
CREATE TABLE IF NOT EXISTS gold.dim_seller (
    seller_sk          BIGSERIAL PRIMARY KEY,              -- clave subrogada (una por versión)
    seller_id          BIGINT  NOT NULL,                   -- clave natural (id de Mercado Libre)
    site_id            TEXT    NOT NULL REFERENCES gold.dim_site (site_id),
    nickname           TEXT,
    is_official_store  BOOLEAN NOT NULL,
    row_hash           TEXT    NOT NULL,                   -- detecta cambios en los atributos
    valid_from         DATE    NOT NULL,
    valid_to           DATE    NOT NULL DEFAULT DATE '9999-12-31',
    is_current         BOOLEAN NOT NULL DEFAULT TRUE,
    CHECK (valid_from < valid_to)
);
-- Garantiza una sola versión vigente por vendedor.
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_seller_current ON gold.dim_seller (seller_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS ix_dim_seller_natural ON gold.dim_seller (seller_id, valid_from);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key     INTEGER PRIMARY KEY,           -- YYYYMMDD
    full_date    DATE    NOT NULL UNIQUE,
    year         SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,             -- 1 = lunes
    is_weekend   BOOLEAN NOT NULL
);

-- Tabla de hechos. Grano: una publicación por día.
CREATE TABLE IF NOT EXISTS gold.fact_item_daily (
    date_key            INTEGER       NOT NULL REFERENCES gold.dim_date (date_key),
    item_id             TEXT          NOT NULL,
    site_id             TEXT          NOT NULL REFERENCES gold.dim_site (site_id),
    category_id         TEXT          NOT NULL REFERENCES gold.dim_category (category_id),
    seller_sk           BIGINT        NOT NULL REFERENCES gold.dim_seller (seller_sk),
    seller_id           BIGINT        NOT NULL,             -- clave natural (comodidad para análisis)
    currency_id         TEXT          NOT NULL,
    price_local         NUMERIC(18,2) NOT NULL,
    price_usd           NUMERIC(18,2) NOT NULL,
    discount_pct        NUMERIC(6,2),
    available_quantity  INTEGER,
    free_shipping       BOOLEAN,
    is_new              BOOLEAN,
    _run_id             TEXT          NOT NULL,
    PRIMARY KEY (date_key, item_id)
);

-- Data mart listo para BI: métricas por país, categoría y día.
CREATE TABLE IF NOT EXISTS gold.mart_category_daily (
    full_date            DATE          NOT NULL,
    site_id              TEXT          NOT NULL,
    country              TEXT          NOT NULL,
    category_id          TEXT          NOT NULL,
    category_name        TEXT          NOT NULL,
    items                INTEGER       NOT NULL,
    sellers              INTEGER       NOT NULL,
    avg_price_usd        NUMERIC(18,2) NOT NULL,
    median_price_usd     NUMERIC(18,2) NOT NULL,
    p25_price_usd        NUMERIC(18,2) NOT NULL,
    p75_price_usd        NUMERIC(18,2) NOT NULL,
    pct_free_shipping    NUMERIC(5,2)  NOT NULL,
    avg_discount_pct     NUMERIC(6,2),
    _run_id              TEXT          NOT NULL,
    PRIMARY KEY (full_date, site_id, category_id)
);
