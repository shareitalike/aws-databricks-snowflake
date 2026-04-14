-- =============================================================================
-- Snowflake Analytics Schema - Star Schema Design
CREATE DATABASE IF NOT EXISTS EVENT_ANALYTICS;
USE DATABASE EVENT_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- ---------------------------------------------------------------------------
-- Step 2: Create warehouse (if not exists)
-- ---------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_XS
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE COMPUTE_XS;

-- ===========================================================================
-- DIMENSION TABLES
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        DATE        PRIMARY KEY,
    year            INT         NOT NULL,
    month           INT         NOT NULL,
    day             INT         NOT NULL,
    day_of_week     INT         NOT NULL,   -- 0=Monday, 6=Sunday
    day_name        VARCHAR(10) NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    quarter         INT         NOT NULL,
    is_weekend      BOOLEAN     NOT NULL,
    week_of_year    INT         NOT NULL
);

-- Populate dim_date for 2024 (extend range as needed)
INSERT INTO dim_date
SELECT
    date_key,
    YEAR(date_key),
    MONTH(date_key),
    DAY(date_key),
    DAYOFWEEK(date_key),
    DAYNAME(date_key),
    MONTHNAME(date_key),
    QUARTER(date_key),
    CASE WHEN DAYOFWEEK(date_key) IN (0, 6) THEN TRUE ELSE FALSE END,
    WEEKOFYEAR(date_key)
FROM (
    SELECT DATEADD(day, seq4(), '2024-01-01')::DATE AS date_key
    FROM TABLE(GENERATOR(ROWCOUNT => 730))  -- 2 years
)
WHERE date_key NOT IN (SELECT date_key FROM dim_date);

-- ---------------------------------------------------------------------------
-- dim_users - User dimension (derived from events)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_users (
    user_id         INT         PRIMARY KEY,
    first_seen      TIMESTAMP   NOT NULL,
    last_seen       TIMESTAMP   NOT NULL,
    primary_device  VARCHAR(20),
    primary_country VARCHAR(5),
    total_events    INT         DEFAULT 0,
    total_purchases INT         DEFAULT 0,
    total_revenue   DECIMAL(12,2) DEFAULT 0.00,
    updated_at      TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- dim_products - Product dimension (derived from events)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_products (
    product_id          INT             PRIMARY KEY,
    first_seen          TIMESTAMP       NOT NULL,
    last_seen           TIMESTAMP       NOT NULL,
    avg_price           DECIMAL(10,2),
    total_views         INT             DEFAULT 0,
    total_cart_adds     INT             DEFAULT 0,
    total_purchases     INT             DEFAULT 0,
    conversion_rate     DECIMAL(5,2)    DEFAULT 0.00,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);


-- ===========================================================================
-- FACT TABLE
CREATE TABLE IF NOT EXISTS fact_events (
    event_id                VARCHAR(36)     NOT NULL,
    event_time              TIMESTAMP       NOT NULL,
    event_date              DATE            NOT NULL,
    user_id                 INT             NOT NULL,
    event_type              VARCHAR(20)     NOT NULL,
    product_id              INT,
    price                   DECIMAL(10,2),
    device                  VARCHAR(20)     NOT NULL,
    country                 VARCHAR(5)      NOT NULL,
    ingestion_timestamp     TIMESTAMP,
    processing_timestamp    TIMESTAMP,
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (event_date, event_type);
CREATE TABLE IF NOT EXISTS gold_daily_active_users (
    event_date          DATE        NOT NULL,
    daily_active_users  INT         NOT NULL,
    total_events        INT         NOT NULL,
    purchasing_users    INT         DEFAULT 0,
    loaded_at           TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS gold_conversion_funnel (
    event_date              DATE            NOT NULL,
    viewers                 INT             NOT NULL,
    cart_adders             INT             NOT NULL,
    purchasers              INT             NOT NULL,
    view_to_cart_pct        DECIMAL(5,2),
    cart_to_purchase_pct    DECIMAL(5,2),
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS gold_daily_revenue (
    event_date      DATE            NOT NULL,
    purchase_count  INT             NOT NULL,
    total_revenue   DECIMAL(12,2)   NOT NULL,
    avg_order_value DECIMAL(10,2),
    min_order       DECIMAL(10,2),
    max_order       DECIMAL(10,2),
    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS gold_top_products (
    event_date          DATE    NOT NULL,
    product_id          INT     NOT NULL,
    total_interactions  INT     NOT NULL,
    unique_users        INT,
    views               INT,
    cart_adds           INT,
    purchases           INT,
    revenue             DECIMAL(12,2),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS gold_events_by_device (
    event_date      DATE            NOT NULL,
    device          VARCHAR(20)     NOT NULL,
    event_count     INT             NOT NULL,
    unique_users    INT,
    purchases       INT,
    revenue         DECIMAL(12,2),
    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);
