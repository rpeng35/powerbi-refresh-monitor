-- =============================================================================
-- Sample Analytical Queries for Power BI Refresh History
-- =============================================================================
-- Run these in Databricks SQL or a notebook %sql cell.
-- Replace 'your_catalog.your_schema.powerbi_refresh_history' with your table name.
-- =============================================================================

-- 1. Latest refresh status for each dataset
SELECT
    dataset_name,
    workspace_name,
    status,
    refresh_type,
    start_time,
    end_time,
    ROUND(duration_seconds / 60.0, 1) AS duration_minutes,
    error_code
FROM your_catalog.your_schema.powerbi_refresh_history
WHERE (dataset_id, start_time) IN (
    SELECT dataset_id, MAX(start_time)
    FROM your_catalog.your_schema.powerbi_refresh_history
    GROUP BY dataset_id
)
ORDER BY is_critical DESC, dataset_name;


-- 2. Daily success/failure counts over the last 30 days
SELECT
    ingestion_date,
    status,
    COUNT(*) AS refresh_count
FROM your_catalog.your_schema.powerbi_refresh_history
WHERE ingestion_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ingestion_date, status
ORDER BY ingestion_date DESC, status;


-- 3. Average refresh duration by dataset (last 7 days)
SELECT
    dataset_name,
    workspace_name,
    COUNT(*) AS completed_refreshes,
    ROUND(AVG(duration_seconds) / 60.0, 1) AS avg_duration_minutes,
    ROUND(MAX(duration_seconds) / 60.0, 1) AS max_duration_minutes,
    ROUND(MIN(duration_seconds) / 60.0, 1) AS min_duration_minutes
FROM your_catalog.your_schema.powerbi_refresh_history
WHERE status = 'Completed'
  AND start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY dataset_name, workspace_name
ORDER BY avg_duration_minutes DESC;


-- 4. Refresh duration trend for a specific dataset
SELECT
    DATE(start_time) AS refresh_date,
    ROUND(AVG(duration_seconds) / 60.0, 1) AS avg_duration_minutes,
    COUNT(*) AS refresh_count
FROM your_catalog.your_schema.powerbi_refresh_history
WHERE dataset_name = 'Monthly Revenue Model'
  AND status = 'Completed'
  AND start_time >= current_date() - INTERVAL 30 DAYS
GROUP BY DATE(start_time)
ORDER BY refresh_date;


-- 5. All failures with error details (last 7 days)
SELECT
    dataset_name,
    workspace_name,
    start_time,
    error_code,
    service_exception_json,
    is_critical
FROM your_catalog.your_schema.powerbi_refresh_history
WHERE status = 'Failed'
  AND start_time >= current_date() - INTERVAL 7 DAYS
ORDER BY is_critical DESC, start_time DESC;


-- 6. Data freshness report (hours since last successful refresh per dataset)
SELECT
    dataset_name,
    workspace_name,
    is_critical,
    MAX(end_time) AS last_successful_refresh,
    ROUND(
        (UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(MAX(end_time))) / 3600.0, 1
    ) AS hours_since_refresh
FROM your_catalog.your_schema.powerbi_refresh_history
WHERE status = 'Completed'
GROUP BY dataset_name, workspace_name, is_critical
ORDER BY is_critical DESC, hours_since_refresh DESC;


-- 7. Datasets with increasing refresh durations (week-over-week comparison)
WITH weekly AS (
    SELECT
        dataset_name,
        CASE
            WHEN start_time >= current_date() - INTERVAL 7 DAYS THEN 'this_week'
            ELSE 'last_week'
        END AS period,
        AVG(duration_seconds) AS avg_duration
    FROM your_catalog.your_schema.powerbi_refresh_history
    WHERE status = 'Completed'
      AND start_time >= current_date() - INTERVAL 14 DAYS
    GROUP BY dataset_name,
        CASE WHEN start_time >= current_date() - INTERVAL 7 DAYS THEN 'this_week' ELSE 'last_week' END
)
SELECT
    tw.dataset_name,
    ROUND(lw.avg_duration / 60.0, 1) AS last_week_avg_min,
    ROUND(tw.avg_duration / 60.0, 1) AS this_week_avg_min,
    ROUND((tw.avg_duration - lw.avg_duration) / lw.avg_duration * 100, 1) AS pct_change
FROM weekly tw
JOIN weekly lw ON tw.dataset_name = lw.dataset_name
WHERE tw.period = 'this_week' AND lw.period = 'last_week'
  AND tw.avg_duration > lw.avg_duration
ORDER BY pct_change DESC;


-- 8. Refresh schedule overlap detection (concurrent refreshes)
SELECT
    a.dataset_name AS dataset_a,
    b.dataset_name AS dataset_b,
    a.start_time AS a_start,
    a.end_time AS a_end,
    b.start_time AS b_start,
    b.end_time AS b_end
FROM your_catalog.your_schema.powerbi_refresh_history a
JOIN your_catalog.your_schema.powerbi_refresh_history b
    ON a.request_id < b.request_id
    AND a.start_time < b.end_time
    AND b.start_time < a.end_time
WHERE a.status = 'Completed'
  AND b.status = 'Completed'
  AND a.start_time >= current_date() - INTERVAL 1 DAY
ORDER BY a.start_time;
