-- ============================================================
-- PROJECT: Kenya Agricultural Market Analysis (2019–2023)
-- Author : Kipkoech Hassan Bett
-- Tools  : SQL (SQLite / PostgreSQL compatible)
-- Dataset: 950 records | 10 regions | 10 crops | 5 years
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- SECTION 1: DATABASE SETUP
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS agri_data (
    record_id               INTEGER PRIMARY KEY,
    region                  TEXT,
    crop                    TEXT,
    year                    INTEGER,
    season                  TEXT,
    area_planted_ha         REAL,
    yield_tonnes_ha         REAL,
    total_production_tonnes REAL,
    market_price_kes_kg     REAL,
    total_revenue_kes       REAL,
    fertilizer_use_pct      REAL,
    smallholder_count       INTEGER
);

-- NOTE: After creating the table, import kenya_agri_raw.csv
-- In SQLite:  .mode csv
--             .import kenya_agri_raw.csv agri_data


-- ────────────────────────────────────────────────────────────
-- SECTION 2: DATA EXPLORATION & QUALITY CHECK
-- ────────────────────────────────────────────────────────────

-- 2.1 Overview: row count and date range
SELECT
    COUNT(*)        AS total_records,
    MIN(year)       AS earliest_year,
    MAX(year)       AS latest_year,
    COUNT(DISTINCT region) AS regions,
    COUNT(DISTINCT crop)   AS crops
FROM agri_data;

-- 2.2 Check for NULLs in key columns
SELECT
    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END)                  AS null_region,
    SUM(CASE WHEN yield_tonnes_ha IS NULL THEN 1 ELSE 0 END)         AS null_yield,
    SUM(CASE WHEN total_revenue_kes IS NULL THEN 1 ELSE 0 END)       AS null_revenue,
    SUM(CASE WHEN market_price_kes_kg IS NULL THEN 1 ELSE 0 END)     AS null_price
FROM agri_data;

-- 2.3 Check for zero or negative values (data quality)
SELECT COUNT(*) AS suspicious_rows
FROM agri_data
WHERE yield_tonnes_ha <= 0
   OR area_planted_ha <= 0
   OR total_revenue_kes <= 0;

-- 2.4 Summary statistics per crop
SELECT
    crop,
    COUNT(*)                                    AS records,
    ROUND(AVG(yield_tonnes_ha), 2)              AS avg_yield_t_ha,
    ROUND(MIN(yield_tonnes_ha), 2)              AS min_yield,
    ROUND(MAX(yield_tonnes_ha), 2)              AS max_yield,
    ROUND(AVG(market_price_kes_kg), 1)          AS avg_price_kes,
    ROUND(AVG(fertilizer_use_pct), 1)           AS avg_fertilizer_pct
FROM agri_data
GROUP BY crop
ORDER BY avg_yield_t_ha DESC;


-- ────────────────────────────────────────────────────────────
-- SECTION 3: INSIGHT 1 — WHICH CROPS DRIVE THE MOST REVENUE?
-- ────────────────────────────────────────────────────────────

-- 3.1 Total revenue by crop (all years combined)
SELECT
    crop,
    ROUND(SUM(total_revenue_kes) / 1e6, 1)     AS total_revenue_M_KES,
    ROUND(AVG(market_price_kes_kg), 1)          AS avg_price_kes_kg,
    ROUND(SUM(total_production_tonnes), 0)      AS total_production_tonnes,
    ROUND(AVG(yield_tonnes_ha), 2)              AS avg_yield_t_ha
FROM agri_data
GROUP BY crop
ORDER BY total_revenue_M_KES DESC;

-- 3.2 Top 3 revenue-generating crops per region
WITH ranked AS (
    SELECT
        region,
        crop,
        ROUND(SUM(total_revenue_kes) / 1e6, 1) AS revenue_M_KES,
        RANK() OVER (
            PARTITION BY region
            ORDER BY SUM(total_revenue_kes) DESC
        ) AS revenue_rank
    FROM agri_data
    GROUP BY region, crop
)
SELECT region, crop, revenue_M_KES, revenue_rank
FROM ranked
WHERE revenue_rank <= 3
ORDER BY region, revenue_rank;

-- 3.3 Revenue concentration — what % of total comes from top 3 crops?
WITH crop_totals AS (
    SELECT
        crop,
        SUM(total_revenue_kes) AS revenue
    FROM agri_data
    GROUP BY crop
),
grand_total AS (
    SELECT SUM(total_revenue_kes) AS total FROM agri_data
)
SELECT
    c.crop,
    ROUND(c.revenue / 1e6, 1)                          AS revenue_M_KES,
    ROUND(c.revenue * 100.0 / g.total, 1)              AS pct_of_total
FROM crop_totals c, grand_total g
ORDER BY revenue DESC
LIMIT 5;


-- ────────────────────────────────────────────────────────────
-- SECTION 4: INSIGHT 2 — PRICE VOLATILITY TRENDS (2019–2023)
-- ────────────────────────────────────────────────────────────

-- 4.1 Average price per crop per year
SELECT
    year,
    crop,
    ROUND(AVG(market_price_kes_kg), 1) AS avg_price_kes_kg
FROM agri_data
GROUP BY year, crop
ORDER BY crop, year;

-- 4.2 Year-over-year price change per crop
WITH yearly_price AS (
    SELECT
        crop,
        year,
        ROUND(AVG(market_price_kes_kg), 1) AS avg_price
    FROM agri_data
    GROUP BY crop, year
)
SELECT
    curr.crop,
    curr.year,
    curr.avg_price                                                  AS price_this_year,
    prev.avg_price                                                  AS price_prev_year,
    ROUND(curr.avg_price - prev.avg_price, 1)                      AS price_change,
    ROUND((curr.avg_price - prev.avg_price) * 100.0
          / prev.avg_price, 1)                                     AS pct_change
FROM yearly_price curr
LEFT JOIN yearly_price prev
    ON curr.crop = prev.crop AND curr.year = prev.year + 1
WHERE prev.avg_price IS NOT NULL
ORDER BY ABS(pct_change) DESC;

-- 4.3 Most volatile crops (highest std deviation in price)
-- Using variance approximation: AVG(price^2) - AVG(price)^2
SELECT
    crop,
    ROUND(AVG(market_price_kes_kg), 1)                             AS avg_price,
    ROUND(MAX(market_price_kes_kg) - MIN(market_price_kes_kg), 1) AS price_range,
    ROUND(MAX(market_price_kes_kg), 1)                             AS max_price,
    ROUND(MIN(market_price_kes_kg), 1)                             AS min_price
FROM agri_data
GROUP BY crop
ORDER BY price_range DESC;

-- 4.4 Impact of 2022 drought on production vs 2021
SELECT
    crop,
    ROUND(SUM(CASE WHEN year = 2021 THEN total_production_tonnes END), 0) AS production_2021,
    ROUND(SUM(CASE WHEN year = 2022 THEN total_production_tonnes END), 0) AS production_2022,
    ROUND(
        (SUM(CASE WHEN year = 2022 THEN total_production_tonnes END) -
         SUM(CASE WHEN year = 2021 THEN total_production_tonnes END))
        * 100.0 /
        NULLIF(SUM(CASE WHEN year = 2021 THEN total_production_tonnes END), 0)
    , 1)                                                           AS pct_change_2022
FROM agri_data
GROUP BY crop
ORDER BY pct_change_2022 ASC;


-- ────────────────────────────────────────────────────────────
-- SECTION 5: INSIGHT 3 — PRODUCTIVITY GAPS (OPPORTUNITY AREAS)
-- ────────────────────────────────────────────────────────────

-- 5.1 Average vs top-quartile yield per crop (gap = opportunity)
WITH stats AS (
    SELECT
        crop,
        AVG(yield_tonnes_ha)                    AS avg_yield,
        -- Top quartile approximation: avg of top 25% records
        AVG(CASE
            WHEN yield_tonnes_ha >= (
                SELECT AVG(y2)
                FROM (
                    SELECT yield_tonnes_ha AS y2
                    FROM agri_data a2
                    WHERE a2.crop = agri_data.crop
                    ORDER BY yield_tonnes_ha DESC
                    LIMIT MAX(1, CAST(COUNT(*) * 0.25 AS INT))
                )
            ) THEN yield_tonnes_ha END)          AS top_yield_approx
    FROM agri_data
    GROUP BY crop
)
SELECT
    crop,
    ROUND(avg_yield, 2)                         AS avg_yield_t_ha,
    ROUND(MAX(y.yield_tonnes_ha), 2)            AS best_yield_recorded,
    ROUND(MAX(y.yield_tonnes_ha) - avg_yield, 2) AS yield_gap,
    ROUND((MAX(y.yield_tonnes_ha) - avg_yield)
          * 100.0 / MAX(y.yield_tonnes_ha), 1)  AS gap_pct
FROM stats s
JOIN agri_data y USING (crop)
GROUP BY crop, avg_yield
ORDER BY gap_pct DESC;

-- 5.2 Regions with lowest yield per crop vs national average
WITH national_avg AS (
    SELECT crop, AVG(yield_tonnes_ha) AS nat_avg_yield
    FROM agri_data
    GROUP BY crop
),
regional_avg AS (
    SELECT region, crop, AVG(yield_tonnes_ha) AS reg_avg_yield
    FROM agri_data
    GROUP BY region, crop
)
SELECT
    r.region,
    r.crop,
    ROUND(r.reg_avg_yield, 2)                   AS regional_yield,
    ROUND(n.nat_avg_yield, 2)                   AS national_avg_yield,
    ROUND(r.reg_avg_yield - n.nat_avg_yield, 2) AS diff_from_national,
    CASE
        WHEN r.reg_avg_yield < n.nat_avg_yield * 0.85
        THEN 'HIGH OPPORTUNITY'
        WHEN r.reg_avg_yield < n.nat_avg_yield
        THEN 'Below Average'
        ELSE 'Above Average'
    END                                          AS opportunity_flag
FROM regional_avg r
JOIN national_avg n ON r.crop = n.crop
ORDER BY diff_from_national ASC
LIMIT 20;

-- 5.3 Fertilizer use vs yield correlation (proxy)
SELECT
    CASE
        WHEN fertilizer_use_pct < 45 THEN 'Low (<45%)'
        WHEN fertilizer_use_pct < 65 THEN 'Medium (45-65%)'
        ELSE 'High (>65%)'
    END                                          AS fertilizer_tier,
    COUNT(*)                                     AS records,
    ROUND(AVG(yield_tonnes_ha), 2)               AS avg_yield,
    ROUND(AVG(total_revenue_kes) / 1e3, 1)       AS avg_revenue_K_KES
FROM agri_data
GROUP BY fertilizer_tier
ORDER BY avg_yield DESC;


-- ────────────────────────────────────────────────────────────
-- SECTION 6: REGIONAL PERFORMANCE SUMMARY
-- ────────────────────────────────────────────────────────────

-- 6.1 Overall regional scorecard
SELECT
    region,
    ROUND(SUM(total_revenue_kes) / 1e6, 1)     AS total_revenue_M_KES,
    ROUND(AVG(yield_tonnes_ha), 2)              AS avg_yield_t_ha,
    ROUND(AVG(market_price_kes_kg), 1)          AS avg_price_kes_kg,
    SUM(smallholder_count)                      AS total_smallholders,
    ROUND(AVG(fertilizer_use_pct), 1)           AS avg_fertilizer_pct,
    COUNT(DISTINCT crop)                        AS crop_diversity
FROM agri_data
GROUP BY region
ORDER BY total_revenue_M_KES DESC;

-- 6.2 Best performing region-crop combination
SELECT
    region,
    crop,
    ROUND(SUM(total_revenue_kes) / 1e6, 1)     AS total_revenue_M_KES,
    ROUND(AVG(yield_tonnes_ha), 2)              AS avg_yield,
    ROUND(AVG(market_price_kes_kg), 1)          AS avg_price
FROM agri_data
GROUP BY region, crop
ORDER BY total_revenue_M_KES DESC
LIMIT 15;


-- ────────────────────────────────────────────────────────────
-- SECTION 7: FINAL INSIGHT SUMMARY (DASHBOARD PREP)
-- ────────────────────────────────────────────────────────────

-- KPIs for Power BI dashboard cards
SELECT
    ROUND(SUM(total_revenue_kes) / 1e9, 2)         AS total_revenue_B_KES,
    ROUND(AVG(yield_tonnes_ha), 2)                  AS overall_avg_yield,
    ROUND(SUM(total_production_tonnes) / 1e6, 2)    AS total_production_M_tonnes,
    SUM(smallholder_count)                          AS total_smallholders_supported,
    COUNT(DISTINCT region)                          AS regions_covered,
    COUNT(DISTINCT crop)                            AS crops_tracked,
    MIN(year) || ' – ' || MAX(year)                 AS period
FROM agri_data;
