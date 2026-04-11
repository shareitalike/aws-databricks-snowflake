-- =============================================================================
-- Snowflake Data Loading - S3 Gold Layer → Snowflake Tables
-- =============================================================================
-- INTERVIEW NOTE: Snowflake reads directly from S3 using external stages.
-- No ETL tool needed - Snowflake's COPY INTO command handles S3 → table loading
-- with exactly-once semantics (tracks loaded files, skips duplicates).
--
-- Alternative approaches:
--   - Snowpipe (auto-ingest on S3 event) - better for real-time, costs more
--   - 3rd party ETL (Fivetran, Airbyte) - managed but expensive
--   - COPY INTO (manual/scheduled) - cheapest, sufficient for batch loads
-- =============================================================================

USE DATABASE EVENT_ANALYTICS;
USE SCHEMA ANALYTICS;
USE WAREHOUSE COMPUTE_XS;

-- ===========================================================================
-- Step 1: Create S3 External Stage
-- ===========================================================================
-- INTERVIEW NOTE: A Snowflake STAGE is a pointer to an external location (S3).
-- It stores the S3 path, credentials, and file format so you don't repeat
-- them in every COPY INTO command.

-- Option A: Using Storage Integration (recommended for production)
-- CREATE STORAGE INTEGRATION s3_integration
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = S3
--     ENABLED = TRUE
--     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT:role/snowflake-s3-role'
--     STORAGE_ALLOWED_LOCATIONS = ('s3://YOUR-BUCKET-NAME/');

-- Option B: Using AWS keys directly (simpler for portfolio, less secure)
CREATE OR REPLACE STAGE gold_stage
    URL = 's3://YOUR-BUCKET-NAME/gold/'
    CREDENTIALS = (
        AWS_KEY_ID = 'YOUR_ACCESS_KEY'
        AWS_SECRET_KEY = 'YOUR_SECRET_KEY'
    )
    FILE_FORMAT = (
        TYPE = PARQUET
        COMPRESSION = SNAPPY
    );

-- Verify stage works
LIST @gold_stage;


-- ===========================================================================
-- Step 2: Load Gold Tables from Delta Lake / Parquet
-- ===========================================================================
-- INTERVIEW NOTE: COPY INTO is idempotent by default. Snowflake tracks which
-- files have been loaded (in metadata). Running COPY INTO twice on the same
-- files skips already-loaded files. This prevents duplicates without any
-- custom dedup logic. To force reload: COPY INTO ... FORCE = TRUE.

-- ---------------------------------------------------------------------------
-- Load Daily Active Users
-- ---------------------------------------------------------------------------
COPY INTO gold_daily_active_users (
    event_date, daily_active_users, total_events, purchasing_users, loaded_at
)
FROM (
    SELECT
        $1:event_date::DATE,
        $1:daily_active_users::INT,
        $1:total_events::INT,
        $1:purchasing_users::INT,
        CURRENT_TIMESTAMP()
    FROM @gold_stage/daily_active_users/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- ---------------------------------------------------------------------------
-- Load Conversion Funnel
-- ---------------------------------------------------------------------------
COPY INTO gold_conversion_funnel (
    event_date, viewers, cart_adders, purchasers,
    view_to_cart_pct, cart_to_purchase_pct, loaded_at
)
FROM (
    SELECT
        $1:event_date::DATE,
        $1:viewers::INT,
        $1:cart_adders::INT,
        $1:purchasers::INT,
        $1:view_to_cart_pct::DECIMAL(5,2),
        $1:cart_to_purchase_pct::DECIMAL(5,2),
        CURRENT_TIMESTAMP()
    FROM @gold_stage/conversion_funnel/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- ---------------------------------------------------------------------------
-- Load Daily Revenue
-- ---------------------------------------------------------------------------
COPY INTO gold_daily_revenue (
    event_date, purchase_count, total_revenue,
    avg_order_value, min_order, max_order, loaded_at
)
FROM (
    SELECT
        $1:event_date::DATE,
        $1:purchase_count::INT,
        $1:total_revenue::DECIMAL(12,2),
        $1:avg_order_value::DECIMAL(10,2),
        $1:min_order::DECIMAL(10,2),
        $1:max_order::DECIMAL(10,2),
        CURRENT_TIMESTAMP()
    FROM @gold_stage/daily_revenue/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- ---------------------------------------------------------------------------
-- Load Top Products
-- ---------------------------------------------------------------------------
COPY INTO gold_top_products (
    event_date, product_id, total_interactions, unique_users,
    views, cart_adds, purchases, revenue, loaded_at
)
FROM (
    SELECT
        $1:event_date::DATE,
        $1:product_id::INT,
        $1:total_interactions::INT,
        $1:unique_users::INT,
        $1:views::INT,
        $1:cart_adds::INT,
        $1:purchases::INT,
        $1:revenue::DECIMAL(12,2),
        CURRENT_TIMESTAMP()
    FROM @gold_stage/top_products/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- ---------------------------------------------------------------------------
-- Load Events by Device
-- ---------------------------------------------------------------------------
COPY INTO gold_events_by_device (
    event_date, device, event_count, unique_users,
    purchases, revenue, loaded_at
)
FROM (
    SELECT
        $1:event_date::DATE,
        $1:device::VARCHAR(20),
        $1:event_count::INT,
        $1:unique_users::INT,
        $1:purchases::INT,
        $1:revenue::DECIMAL(12,2),
        CURRENT_TIMESTAMP()
    FROM @gold_stage/events_by_device/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;


-- ===========================================================================
-- Step 3: Populate Dimension Tables from Fact Data
-- ===========================================================================
-- INTERVIEW NOTE: In production, dimensions come from source systems (CRM,
-- product catalog). Here we derive them from fact_events since we only have
-- event data. MERGE ensures idempotent updates - running twice = same result.

-- ---------------------------------------------------------------------------
-- Populate dim_users (MERGE for incremental update)
-- ---------------------------------------------------------------------------
MERGE INTO dim_users AS target
USING (
    SELECT
        user_id,
        MIN(event_time)     AS first_seen,
        MAX(event_time)     AS last_seen,
        MODE(device)        AS primary_device,
        MODE(country)       AS primary_country,
        COUNT(*)            AS total_events,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,
        COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN price END), 0) AS total_revenue
    FROM fact_events
    GROUP BY user_id
) AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN UPDATE SET
    last_seen = source.last_seen,
    primary_device = source.primary_device,
    primary_country = source.primary_country,
    total_events = source.total_events,
    total_purchases = source.total_purchases,
    total_revenue = source.total_revenue,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    user_id, first_seen, last_seen, primary_device, primary_country,
    total_events, total_purchases, total_revenue, updated_at
) VALUES (
    source.user_id, source.first_seen, source.last_seen,
    source.primary_device, source.primary_country,
    source.total_events, source.total_purchases, source.total_revenue,
    CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- Populate dim_products (MERGE for incremental update)
-- ---------------------------------------------------------------------------
MERGE INTO dim_products AS target
USING (
    SELECT
        product_id,
        MIN(event_time)     AS first_seen,
        MAX(event_time)     AS last_seen,
        AVG(price)          AS avg_price,
        COUNT(CASE WHEN event_type = 'product_view' THEN 1 END) AS total_views,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS total_cart_adds,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,
        ROUND(
            NULLIF(COUNT(CASE WHEN event_type = 'purchase' THEN 1 END), 0) * 100.0 /
            NULLIF(COUNT(CASE WHEN event_type = 'product_view' THEN 1 END), 0),
            2
        ) AS conversion_rate
    FROM fact_events
    WHERE product_id IS NOT NULL
    GROUP BY product_id
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    last_seen = source.last_seen,
    avg_price = source.avg_price,
    total_views = source.total_views,
    total_cart_adds = source.total_cart_adds,
    total_purchases = source.total_purchases,
    conversion_rate = source.conversion_rate,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    product_id, first_seen, last_seen, avg_price,
    total_views, total_cart_adds, total_purchases, conversion_rate, updated_at
) VALUES (
    source.product_id, source.first_seen, source.last_seen, source.avg_price,
    source.total_views, source.total_cart_adds, source.total_purchases,
    source.conversion_rate, CURRENT_TIMESTAMP()
);


-- ===========================================================================
-- Step 4: Verify loads
-- ===========================================================================
SELECT 'gold_daily_active_users' AS table_name, COUNT(*) AS rows FROM gold_daily_active_users
UNION ALL
SELECT 'gold_conversion_funnel', COUNT(*) FROM gold_conversion_funnel
UNION ALL
SELECT 'gold_daily_revenue', COUNT(*) FROM gold_daily_revenue
UNION ALL
SELECT 'gold_top_products', COUNT(*) FROM gold_top_products
UNION ALL
SELECT 'gold_events_by_device', COUNT(*) FROM gold_events_by_device
UNION ALL
SELECT 'dim_users', COUNT(*) FROM dim_users
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;
