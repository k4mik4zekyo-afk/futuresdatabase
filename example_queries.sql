-- Example SQL Queries for Market Data Archivist
-- These queries demonstrate common patterns for analyzing market data
-- and annotations stored in the SQLite database.

-- =============================================================================
-- SCHEMA EXPLORATION
-- =============================================================================

-- List all tables in the database
SELECT name FROM sqlite_master WHERE type='table';

-- Show structure of trade_days table
PRAGMA table_info(trade_days);

-- Show structure of bars table
PRAGMA table_info(bars);

-- Show structure of day_annotations table
PRAGMA table_info(day_annotations);

-- Count records in each table
SELECT 'trade_days' as table_name, COUNT(*) as count FROM trade_days
UNION ALL
SELECT 'bars', COUNT(*) FROM bars
UNION ALL
SELECT 'day_annotations', COUNT(*) FROM day_annotations;

-- =============================================================================
-- BASIC QUERIES
-- =============================================================================

-- Get all trade days for a symbol
SELECT session_date, source
FROM trade_days
WHERE symbol = 'ES'
ORDER BY session_date;

-- Get all bars for a specific trade day
SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume,
    halt_period
FROM bars b
JOIN trade_days td ON b.trade_day_id = td.id
WHERE td.symbol = 'ES'
  AND td.session_date = '2024-01-08'
  AND b.halt_period = 0
ORDER BY b.timestamp;

-- Get date range of available data
SELECT 
    symbol,
    MIN(session_date) as first_day,
    MAX(session_date) as last_day,
    COUNT(DISTINCT session_date) as total_days
FROM trade_days
GROUP BY symbol;

-- =============================================================================
-- AGGREGATIONS
-- =============================================================================

-- Calculate daily OHLC from intraday bars
SELECT 
    td.session_date,
    MIN(b.timestamp) as session_start,
    MAX(b.timestamp) as session_end,
    (SELECT open FROM bars WHERE trade_day_id = td.id AND halt_period = 0 ORDER BY timestamp LIMIT 1) as session_open,
    MAX(b.high) as session_high,
    MIN(b.low) as session_low,
    (SELECT close FROM bars WHERE trade_day_id = td.id AND halt_period = 0 ORDER BY timestamp DESC LIMIT 1) as session_close,
    SUM(b.volume) as total_volume,
    COUNT(b.id) as bar_count
FROM bars b
JOIN trade_days td ON b.trade_day_id = td.id
WHERE td.symbol = 'ES'
  AND b.halt_period = 0
GROUP BY td.session_date
ORDER BY td.session_date;

-- Calculate daily range
SELECT 
    td.session_date,
    MAX(b.high) - MIN(b.low) as daily_range,
    (MAX(b.high) - MIN(b.low)) / MIN(b.low) * 100 as range_percent
FROM bars b
JOIN trade_days td ON b.trade_day_id = td.id
WHERE td.symbol = 'ES'
  AND b.halt_period = 0
GROUP BY td.session_date
ORDER BY td.session_date;

-- Find highest volume bars
SELECT 
    td.session_date,
    b.timestamp,
    b.open,
    b.high,
    b.low,
    b.close,
    b.volume
FROM bars b
JOIN trade_days td ON b.trade_day_id = td.id
WHERE td.symbol = 'ES'
  AND b.halt_period = 0
ORDER BY b.volume DESC
LIMIT 20;

-- =============================================================================
-- ANNOTATION QUERIES
-- =============================================================================

-- Get all active annotations
SELECT 
    td.session_date,
    td.symbol,
    da.annotation_type,
    da.content,
    da.tags,
    datetime(da.created_at, 'unixepoch') as created_timestamp
FROM day_annotations da
JOIN trade_days td ON da.trade_day_id = td.id
WHERE da.status = 'active'
ORDER BY td.session_date;

-- Find trade days with specific tag (e.g., "breakout")
SELECT 
    td.session_date,
    td.symbol,
    da.content,
    da.tags
FROM trade_days td
JOIN day_annotations da ON da.trade_day_id = td.id
WHERE da.tags LIKE '%breakout%'
  AND da.status = 'active'
ORDER BY td.session_date;

-- Count annotations by type
SELECT 
    annotation_type,
    COUNT(*) as count
FROM day_annotations
WHERE status = 'active'
GROUP BY annotation_type
ORDER BY count DESC;

-- =============================================================================
-- JOINED QUERIES (Market Data + Annotations)
-- =============================================================================

-- Join daily range with annotations
SELECT 
    td.session_date,
    MAX(b.high) - MIN(b.low) as daily_range,
    da.content as observation,
    da.tags
FROM bars b
JOIN trade_days td ON b.trade_day_id = td.id
LEFT JOIN day_annotations da ON da.trade_day_id = td.id AND da.status = 'active'
WHERE td.symbol = 'ES'
  AND b.halt_period = 0
GROUP BY td.session_date
ORDER BY td.session_date;

-- Compare range on annotated vs non-annotated days
SELECT 
    CASE WHEN da.id IS NOT NULL THEN 'Annotated' ELSE 'Not Annotated' END as category,
    COUNT(DISTINCT td.id) as day_count,
    AVG(MAX(b.high) - MIN(b.low)) as avg_range
FROM bars b
JOIN trade_days td ON b.trade_day_id = td.id
LEFT JOIN day_annotations da ON da.trade_day_id = td.id AND da.status = 'active'
WHERE td.symbol = 'ES'
  AND b.halt_period = 0
GROUP BY td.session_date
HAVING COUNT(*) > 0;

-- =============================================================================
-- HALT PERIOD ANALYSIS
-- =============================================================================

-- Count halt period bars
SELECT 
    COUNT(*) as halt_bars,
    COUNT(DISTINCT trade_day_id) as days_with_halt_data
FROM bars
WHERE halt_period = 1;

-- Get halt period bars for a specific day
SELECT 
    td.session_date,
    b.timestamp,
    datetime(b.timestamp, 'unixepoch') as timestamp_readable,
    b.open,
    b.high,
    b.low,
    b.close,
    b.volume
FROM bars b
LEFT JOIN trade_days td ON b.trade_day_id = td.id
WHERE b.halt_period = 1
ORDER BY b.timestamp;

-- =============================================================================
-- DATA QUALITY CHECKS
-- =============================================================================

-- Find days with missing data (fewer than expected bars)
SELECT 
    td.session_date,
    COUNT(b.id) as bar_count
FROM trade_days td
LEFT JOIN bars b ON b.trade_day_id = td.id AND b.halt_period = 0
WHERE td.symbol = 'ES'
GROUP BY td.session_date
HAVING COUNT(b.id) < 50  -- Adjust threshold as needed
ORDER BY td.session_date;

-- Check for gaps in timestamps (5-minute bars should be ~300 seconds apart)
WITH bar_gaps AS (
    SELECT 
        td.session_date,
        b.timestamp as current_ts,
        LAG(b.timestamp) OVER (PARTITION BY td.id ORDER BY b.timestamp) as prev_ts,
        b.timestamp - LAG(b.timestamp) OVER (PARTITION BY td.id ORDER BY b.timestamp) as gap_seconds
    FROM bars b
    JOIN trade_days td ON b.trade_day_id = td.id
    WHERE b.halt_period = 0
)
SELECT 
    session_date,
    datetime(current_ts, 'unixepoch') as timestamp,
    gap_seconds,
    gap_seconds / 60.0 as gap_minutes
FROM bar_gaps
WHERE gap_seconds > 600  -- Gaps larger than 10 minutes
ORDER BY session_date, current_ts;

-- Find duplicate timestamps
SELECT 
    trade_day_id,
    timestamp,
    COUNT(*) as duplicate_count
FROM bars
GROUP BY trade_day_id, timestamp
HAVING COUNT(*) > 1;

-- =============================================================================
-- ADVANCED PATTERNS
-- =============================================================================

-- Calculate moving statistics (7-day moving average of range)
WITH daily_stats AS (
    SELECT 
        td.session_date,
        MAX(b.high) - MIN(b.low) as daily_range
    FROM bars b
    JOIN trade_days td ON b.trade_day_id = td.id
    WHERE td.symbol = 'ES'
      AND b.halt_period = 0
    GROUP BY td.session_date
)
SELECT 
    d1.session_date,
    d1.daily_range,
    AVG(d2.daily_range) as ma7_range
FROM daily_stats d1
JOIN daily_stats d2 ON d2.session_date <= d1.session_date 
    AND d2.session_date >= date(d1.session_date, '-6 days')
GROUP BY d1.session_date
ORDER BY d1.session_date;

-- Find days where close > open (bullish days) with annotations
SELECT 
    td.session_date,
    open_bar.open as session_open,
    close_bar.close as session_close,
    close_bar.close - open_bar.open as net_change,
    da.content,
    da.tags
FROM trade_days td
JOIN (
    SELECT trade_day_id, open
    FROM bars
    WHERE halt_period = 0
    GROUP BY trade_day_id
    HAVING timestamp = MIN(timestamp)
) open_bar ON open_bar.trade_day_id = td.id
JOIN (
    SELECT trade_day_id, close
    FROM bars
    WHERE halt_period = 0
    GROUP BY trade_day_id
    HAVING timestamp = MAX(timestamp)
) close_bar ON close_bar.trade_day_id = td.id
LEFT JOIN day_annotations da ON da.trade_day_id = td.id AND da.status = 'active'
WHERE close_bar.close > open_bar.open
ORDER BY td.session_date;

-- =============================================================================
-- ANNOTATION VERSIONING
-- =============================================================================

-- View annotation history (including superseded annotations)
SELECT 
    td.session_date,
    da.id,
    da.content,
    da.status,
    da.supersedes_id,
    datetime(da.created_at, 'unixepoch') as created
FROM day_annotations da
JOIN trade_days td ON da.trade_day_id = td.id
WHERE td.session_date = '2024-01-08'
ORDER BY da.created_at;

-- Find all active annotations that have superseded older ones
SELECT 
    td.session_date,
    da_current.content as current_content,
    da_old.content as superseded_content,
    datetime(da_current.created_at, 'unixepoch') as updated_at
FROM day_annotations da_current
JOIN trade_days td ON da_current.trade_day_id = td.id
LEFT JOIN day_annotations da_old ON da_current.supersedes_id = da_old.id
WHERE da_current.status = 'active'
  AND da_current.supersedes_id IS NOT NULL
ORDER BY td.session_date;

-- =============================================================================
-- EXPORT / REPORTING
-- =============================================================================

-- Generate daily summary report
SELECT 
    td.session_date,
    td.symbol,
    COUNT(DISTINCT b.id) as bar_count,
    (SELECT open FROM bars WHERE trade_day_id = td.id AND halt_period = 0 ORDER BY timestamp LIMIT 1) as open,
    MAX(b.high) as high,
    MIN(b.low) as low,
    (SELECT close FROM bars WHERE trade_day_id = td.id AND halt_period = 0 ORDER BY timestamp DESC LIMIT 1) as close,
    SUM(b.volume) as volume,
    GROUP_CONCAT(da.content, '; ') as notes
FROM trade_days td
LEFT JOIN bars b ON b.trade_day_id = td.id AND b.halt_period = 0
LEFT JOIN day_annotations da ON da.trade_day_id = td.id AND da.status = 'active'
WHERE td.symbol = 'ES'
GROUP BY td.session_date
ORDER BY td.session_date;
