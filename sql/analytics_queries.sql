-- =============================================================================
-- Analytics SQL Queries - Snowflake + Athena Compatible
-- =============================================================================
-- These queries work on both Snowflake (silver/gold tables) and Athena (S3 direct).
-- Each query includes: business purpose, partition filter, performance notes.
--
SELECT
    event_date,
    COUNT(DISTINCT user_id)     AS daily_active_users,
    COUNT(*)                    AS total_events
FROM fact_events
WHERE event_date BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY event_date
ORDER BY event_date;

-- Or from pre-computed gold (instant, no recompute)
SELECT * FROM gold_daily_active_users
WHERE event_date BETWEEN '2024-03-01' AND '2024-03-31'
ORDER BY event_date;


-- ---------------------------------------------------------------------------
-- Query 2: Conversion Funnel
-- ---------------------------------------------------------------------------
-- BUSINESS PURPOSE: Measure where users drop off in the purchase journey.
-- view_to_cart_pct < 15% → product pages need work
-- cart_to_purchase_pct < 30% → checkout friction (payment bugs, high shipping cost)
--
-- PERFORMANCE: Conditional aggregation (CASE WHEN inside COUNT) processes
-- all three stages in ONE pass. Alternative: 3 separate queries JOINed -
-- scans data 3 times. Same result, 3x the compute cost.

SELECT
    event_date,
    COUNT(DISTINCT CASE WHEN event_type = 'product_view'
        THEN user_id END)           AS viewers,
    COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart'
        THEN user_id END)           AS cart_adders,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase'
        THEN user_id END)           AS purchasers,
    ROUND(
        cart_adders * 100.0 / NULLIF(viewers, 0), 2
    )                               AS view_to_cart_pct,
    ROUND(
        purchasers * 100.0 / NULLIF(cart_adders, 0), 2
    )                               AS cart_to_purchase_pct
FROM fact_events
WHERE event_date BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY event_date
ORDER BY event_date;


-- ---------------------------------------------------------------------------
-- Query 3: Daily Revenue
-- ---------------------------------------------------------------------------
-- BUSINESS PURPOSE: Track GMV (Gross Merchandise Value). Sudden drops indicate
-- payment gateway issues. Sudden spikes may indicate a sale or fraud.
--
-- PERFORMANCE: Filter event_type = 'purchase' BEFORE aggregation reduces
-- rows by ~90% (only 10% of events are purchases in our distribution).

SELECT
    event_date,
    COUNT(*)                    AS purchase_count,
    SUM(price)                  AS total_revenue,
    ROUND(AVG(price), 2)       AS avg_order_value,
    MIN(price)                  AS min_order,
    MAX(price)                  AS max_order
FROM fact_events
WHERE event_date BETWEEN '2024-03-01' AND '2024-03-31'
  AND event_type = 'purchase'
  AND price IS NOT NULL
GROUP BY event_date
ORDER BY event_date;


-- ---------------------------------------------------------------------------
-- Query 4: Top 10 Products by Revenue
-- ---------------------------------------------------------------------------
-- BUSINESS PURPOSE: Identify best-selling products for inventory planning,
-- marketing budget allocation, and homepage spotlight.
--
-- PERFORMANCE: LIMIT 10 stops the sort early (top-N optimization).
-- Snowflake's optimizer knows it only needs the top 10, so it uses a
-- heap-based partial sort instead of sorting all rows.

SELECT
    product_id,
    COUNT(*)                    AS purchase_count,
    COUNT(DISTINCT user_id)     AS unique_buyers,
    SUM(price)                  AS total_revenue,
    ROUND(AVG(price), 2)       AS avg_price
FROM fact_events
WHERE event_date BETWEEN '2024-03-01' AND '2024-03-31'
  AND event_type = 'purchase'
  AND product_id IS NOT NULL
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- Query 5: Events by Device
-- ---------------------------------------------------------------------------
-- BUSINESS PURPOSE: If mobile generates 55% of traffic but only 20% of
-- revenue, the mobile checkout needs redesign. This is a real product
-- decision that data engineers surface.
--
-- PERFORMANCE: 3 device types = 3-row GROUP BY result. Very efficient.
-- WINDOW function (SUM OVER) computes percentage without a self-join.

SELECT
    device,
    COUNT(*)                    AS event_count,
    COUNT(DISTINCT user_id)     AS unique_users,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2
    )                           AS pct_of_total,
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END)
                                AS revenue,
    ROUND(
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) * 100.0 /
        NULLIF(SUM(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END)) OVER (), 0),
        2
    )                           AS pct_of_revenue
FROM fact_events
WHERE event_date BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY device
ORDER BY event_count DESC;


-- ---------------------------------------------------------------------------
-- Query 6: Duplicate Detection (Data Quality Audit)
-- ---------------------------------------------------------------------------
-- BUSINESS PURPOSE: This should return ZERO rows if the Databricks dedup
-- pipeline is working correctly. Non-zero results indicate a bug in the
-- bronze→silver transformation or a race condition in partition writes.
--
-- PERFORMANCE: GROUP BY + HAVING is the standard pattern. Snowflake
-- applies the HAVING filter during aggregation, not after materializing
-- all groups - so it's memory-efficient.

SELECT
    event_id,
    COUNT(*)                    AS occurrence_count,
    MIN(event_time)             AS first_seen,
    MAX(event_time)             AS last_seen,
    MIN(ingestion_timestamp)    AS first_ingested,
    MAX(ingestion_timestamp)    AS last_ingested
FROM fact_events
WHERE event_date BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- Query 7: Quarantine Rate Trend (Athena - queries S3 directly)
-- ---------------------------------------------------------------------------
-- BUSINESS PURPOSE: Track data quality over time. A rising quarantine rate
-- indicates a producer bug, API change, or upstream system degradation.
-- Normal baseline: ~5% (our intentional bad record rate).
-- Alert threshold: >10% sustained for >1 hour.
--
-- NOTE: This query runs on ATHENA against the quarantine table, not Snowflake.
-- Quarantine data stays in S3 JSON format - no point loading it into Snowflake.

-- Run in Athena:
-- SELECT
--     year, month, day,
--     _quarantine_reason,
--     COUNT(*) AS failure_count,
--     ROUND(
--         COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY year, month, day), 2
--     ) AS pct_of_daily_failures
-- FROM event_analytics.quarantine_events
-- WHERE year = '2024' AND month = '03'
-- GROUP BY year, month, day, _quarantine_reason
-- ORDER BY year, month, day, failure_count DESC;


-- ===========================================================================
-- BONUS: Cross-dimensional analysis using star schema JOINs
SELECT
    d.day_name,
    d.is_weekend,
    f.device,
    COUNT(*)                    AS purchases,
    SUM(f.price)               AS revenue,
    ROUND(AVG(f.price), 2)    AS avg_order_value
FROM fact_events f
JOIN dim_date d ON f.event_date = d.date_key
WHERE f.event_date BETWEEN '2024-03-01' AND '2024-03-31'
  AND f.event_type = 'purchase'
GROUP BY d.day_name, d.is_weekend, f.device
ORDER BY d.is_weekend, revenue DESC;
