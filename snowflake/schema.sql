-- =============================================================================
-- Snowflake Analytics Schema - Star Schema Design
-- =============================================================================
-- INTERVIEW NOTE: Why a star schema in Snowflake on top of Delta Lake gold?
--
-- DECISION: The gold layer in Delta Lake stores pre-aggregated metrics optimized
-- for batch processing. Snowflake adds:
--   1. Sub-second interactive SQL (Athena has 2-5s cold start per query)
--   2. Concurrent multi-user access (BI tools, analysts, dashboards)
--   3. Dimensional modeling (star schema) for self-service analytics
--   4. Built-in caching (query result cache = free repeat queries)
--
-- TRADEOFF: Additional cost (~$2/credit for compute). Justified when:
--   - Multiple analysts query frequently (Athena cost grows linearly with queries)
--   - Sub-second response required (dashboards, real-time reporting)
--   - Complex joins across dimensions (Snowflake optimizer > Athena for joins)
--
-- When Athena is sufficient:
--   - Occasional ad-hoc queries (< 50/day)
--   - Data engineering team only (no self-service need)
--   - Cost-sensitive workloads where 3-5s latency is acceptable
--
-- INTERVIEW NOTE (scaling): This uses X-Small warehouse ($2/credit/hour).
-- At enterprise scale, you'd use MEDIUM/LARGE with auto-scaling multi-cluster
-- warehouses to handle 100+ concurrent dashboard users.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 1: Create database and schema
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS EVENT_ANALYTICS;
USE DATABASE EVENT_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- ---------------------------------------------------------------------------
-- Step 2: Create warehouse (if not exists)
-- ---------------------------------------------------------------------------
-- INTERVIEW NOTE: Auto-suspend at 60s means the warehouse shuts down after
-- 1 minute of inactivity. You pay ZERO when it's suspended. Auto-resume
-- means the next query automatically starts it (adds ~5s cold start).
-- For a portfolio project, this keeps costs near zero.

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_XS
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE COMPUTE_XS;

-- ===========================================================================
-- DIMENSION TABLES
-- ===========================================================================
-- INTERVIEW NOTE: Why star schema (dimensions + facts)?
-- A star schema denormalizes data for query performance:
--   - Fewer JOINs = faster queries
--   - Dimensions are small (lookup tables) → cached in memory
--   - Facts are large (event data) → scanned by Snowflake optimizer
--
-- A normalized 3NF schema minimizes storage but requires many JOINs.
-- For analytics, query speed > storage cost. Snowflake compresses
-- columnar data so heavily that denormalization barely increases storage.

-- ---------------------------------------------------------------------------
-- dim_date - Calendar dimension
-- ---------------------------------------------------------------------------
-- INTERVIEW NOTE: A date dimension is standard in every analytics warehouse.
-- It enables queries like "compare this week vs same week last year" or
-- "filter to business days only" without complex date math in every query.

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
-- INTERVIEW NOTE: In a real system, dim_users would come from a user service
-- database (CRM, Auth system). Here we derive it from event data since we
-- don't have a real user table. This is a common portfolio compromise.

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
-- INTERVIEW NOTE: Same as dim_users - in production, this comes from a
-- product catalog service. Here, we derive from event data.

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
-- ===========================================================================
-- INTERVIEW NOTE: The fact table stores the lowest-grain events. Each row
-- is one user action. Dimension keys link to the dimension tables.
-- CLUSTER BY (event_date, event_type) optimizes Snowflake's micro-partition
-- pruning - queries filtering by date and event type skip irrelevant partitions.

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
-- INTERVIEW NOTE: CLUSTER BY is Snowflake's equivalent of a clustered index.
-- It physically organizes micro-partitions so that rows with similar
-- event_date and event_type values are stored together. This dramatically
-- improves pruning - a query with WHERE event_date = '2024-03-15'
-- AND event_type = 'purchase' may scan 5% of micro-partitions instead of 100%.
-- Unlike traditional indexes, CLUSTER BY has zero maintenance overhead.


-- ===========================================================================
-- GOLD AGGREGATE TABLES (Loaded from Databricks Gold Layer)
-- ===========================================================================

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
