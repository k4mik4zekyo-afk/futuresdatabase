# Market Data Archivist

A Python-based SQLite database system for ingesting, storing, and querying futures market data with trade day resolution and annotation support.

## Purpose

The Market Data Archivist provides a durable, queryable database for:
- **Learning SQL** on real market data
- **Preserving market observations** alongside price data
- **Understanding relational database design** with foreign keys and joins
- **Building a foundation** for trading analytics

## Features

- ✅ **Trade day resolution** - Automatically assigns bars to correct trade days based on PT timezone
- ✅ **Halt period handling** - Flags and separates data during daily halt (2-3 PM PT)
- ✅ **Idempotent ingestion** - Safe to re-run; detects duplicates and conflicts
- ✅ **Annotations** - Store observations, hypotheses, and reviews with versioning
- ✅ **Multi-source support** - TradingView (default), extensible to other sources
- ✅ **SQL-friendly schema** - Designed for learning and exploration

## Quick Start

### 1. Setup

No external dependencies required beyond Python 3.9+ standard library:

```bash
# Clone the repository
git clone <repo-url>
cd futuresdatabase

# Verify Python version (requires 3.9+)
python --version
```

### 2. Initialize Database

```python
from market_archivist import init_database

# Create the database (safe to call repeatedly)
init_database("market_data.db")
```

### 3. Ingest CSV Data

```python
from market_archivist import ingest_csv

# Ingest TradingView CSV export
result = ingest_csv(
    file_path="ES_5min_2024-01-08.csv",
    symbol="ES",
    timeframe="5m",
    source="tradingview",
    db_path="market_data.db"
)

print(f"Inserted: {result['inserted']} bars")
print(f"Skipped: {result['skipped']} bars")
print(f"Conflicts: {result['conflicts']} bars")
```

### 4. Add Annotations

```python
from market_archivist import save_day_annotation

# Add an observation about a trade day
annotation_id = save_day_annotation(
    symbol="ES",
    session_date="2024-01-08",
    content="Strong breakout above 4800, held throughout session",
    annotation_type="observation",
    tags=["breakout", "trend_day"],
    db_path="market_data.db"
)

print(f"Saved annotation #{annotation_id}")
```

### 5. Query Data

```python
from market_archivist import get_bars, get_day_annotations

# Get all bars for a specific trade day
bars = get_bars(
    symbol="ES",
    session_date="2024-01-08",
    db_path="market_data.db"
)

print(f"Retrieved {len(bars)} bars")

# Query annotations by tag
annotations = get_day_annotations(
    symbol="ES",
    start_date="2024-01-01",
    end_date="2024-01-31",
    tags=["breakout"],
    db_path="market_data.db"
)

print(f"Found {len(annotations)} breakout days")
```

## Database Schema

### Tables

**trade_days** - Join anchor for trade sessions
```sql
CREATE TABLE trade_days (
    id INTEGER PRIMARY KEY,
    symbol TEXT,
    session_date TEXT,        -- YYYY-MM-DD (PT trade day)
    source TEXT,
    UNIQUE(symbol, session_date, source)
);
```

**bars** - Normalized market data
```sql
CREATE TABLE bars (
    id INTEGER PRIMARY KEY,
    trade_day_id INTEGER,
    timestamp INTEGER,        -- epoch seconds (PT)
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    halt_period INTEGER,      -- 0 = false, 1 = true
    raw_json TEXT,
    FOREIGN KEY(trade_day_id) REFERENCES trade_days(id)
);
```

**day_annotations** - Human observations and notes
```sql
CREATE TABLE day_annotations (
    id INTEGER PRIMARY KEY,
    trade_day_id INTEGER,
    annotation_type TEXT,     -- observation | hypothesis | review
    content TEXT,
    tags TEXT,                -- JSON array stored as text
    source TEXT,              -- manual | script
    created_at INTEGER,       -- epoch seconds
    supersedes_id INTEGER,
    status TEXT DEFAULT 'active',  -- active | superseded | deprecated
    FOREIGN KEY(trade_day_id) REFERENCES trade_days(id),
    FOREIGN KEY(supersedes_id) REFERENCES day_annotations(id)
);
```

## Trade Day Calendar

### Trading Hours (Pacific Time)

- **Opens:** Sunday 3:00 PM PT
- **Daily Halt:** 2:00 PM - 3:00 PM PT
- **Closes:** Friday 2:00 PM PT
- **No Trading:** Saturday (any data raises error)

### Assignment Rules

| Time Range | Trade Day Assignment |
|------------|---------------------|
| >= 3:00 PM PT | Next calendar day |
| < 2:00 PM PT | Current calendar day |
| 2:00-3:00 PM PT | Halt period (flagged, no trade day) |

### Examples

```
Sunday 2024-01-07 3:00 PM PT    → trade_day = "2024-01-08" (Monday)
Monday 2024-01-08 1:00 PM PT    → trade_day = "2024-01-08" (Monday)
Monday 2024-01-08 2:30 PM PT    → HALT (flagged, stored separately)
Monday 2024-01-08 3:00 PM PT    → trade_day = "2024-01-09" (Tuesday)
Saturday (any time)             → ERROR (invalid)
```

### Session Composition

Trade day **"2024-01-08" (Monday)** contains bars from:
- **Start:** Sunday 2024-01-07 3:00 PM PT
- **End:** Monday 2024-01-08 2:00 PM PT (exclusive)
- **Duration:** 23 hours

## SQL Learning Examples

### Example 1: Get all bars for a trade day

```sql
SELECT 
    b.timestamp,
    b.open,
    b.high,
    b.low,
    b.close,
    b.volume
FROM bars b
JOIN trade_days td ON b.trade_day_id = td.id
WHERE td.symbol = 'ES'
  AND td.session_date = '2024-01-08'
  AND b.halt_period = 0
ORDER BY b.timestamp;
```

### Example 2: Find trade days with specific annotations

```sql
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
```

### Example 3: Calculate daily OHLC from 5-minute bars

```sql
SELECT 
    td.session_date,
    MIN(b.open) as session_open,
    MAX(b.high) as session_high,
    MIN(b.low) as session_low,
    MAX(b.close) as session_close,
    SUM(b.volume) as total_volume
FROM bars b
JOIN trade_days td ON b.trade_day_id = td.id
WHERE td.symbol = 'ES'
  AND b.halt_period = 0
GROUP BY td.session_date
ORDER BY td.session_date;
```

### Example 4: Join market data with annotations

```sql
SELECT 
    td.session_date,
    MAX(b.high) - MIN(b.low) as daily_range,
    da.content as observation,
    da.tags
FROM bars b
JOIN trade_days td ON b.trade_day_id = td.id
LEFT JOIN day_annotations da ON da.trade_day_id = td.id
WHERE td.symbol = 'ES'
  AND da.status = 'active'
  AND b.halt_period = 0
GROUP BY td.session_date
ORDER BY td.session_date;
```

### Example 5: Explore the schema

```sql
-- List all tables
SELECT name FROM sqlite_master WHERE type='table';

-- Show table structure
PRAGMA table_info(bars);

-- Count records per table
SELECT 'trade_days' as table_name, COUNT(*) as count FROM trade_days
UNION ALL
SELECT 'bars', COUNT(*) FROM bars
UNION ALL
SELECT 'day_annotations', COUNT(*) FROM day_annotations;
```

## API Reference

### Core Functions

#### `init_database(db_path: str = "market_data.db") -> None`
Creates the database and tables if they don't exist. Safe to call repeatedly (idempotent).

#### `ingest_csv(file_path, symbol, timeframe, source="tradingview", db_path="market_data.db") -> dict`
Ingests a CSV file of market data into the database. Returns ingestion statistics.

**Returns:**
```python
{
    "inserted": N,      # New bars added
    "skipped": M,       # Exact duplicates skipped
    "conflicts": K,     # Conflicts detected (different OHLCV for same timestamp)
    "conflict_details": [...]
}
```

#### `save_day_annotation(symbol, session_date, content, annotation_type="observation", tags=None, source="manual", supersedes_id=None, db_path="market_data.db") -> int`
Saves an annotation for a specific trade day. Returns the new annotation ID.

#### `get_bars(symbol, session_date=None, start_date=None, end_date=None, timeframe=None, include_halt=False, source="tradingview", db_path="market_data.db") -> list[dict]`
Queries bars from the database. Default excludes halt period bars.

#### `get_day_annotations(symbol, start_date, end_date, tags=None, status="active", annotation_type=None, db_path="market_data.db") -> list[dict]`
Queries annotations for a date range.

#### `get_trade_day(symbol, session_date, source="tradingview", db_path="market_data.db") -> dict | None`
Gets a trade_day record.

#### `resolve_trade_day(timestamp: int) -> str | None`
Given a Unix timestamp (in PT), returns the trade day (YYYY-MM-DD) or None for halt period.

## CSV Format Requirements

### TradingView (Default)

Expected columns:
- `time` - timestamp in format "YYYY-MM-DDTHH:MM:SS-HH:MM" (ISO 8601 with timezone)
- `open` - opening price
- `high` - high price
- `low` - low price
- `close` - closing price
- `Volume` or `volume` - volume

**Important:** All timestamps must be in Pacific Time (PT).

### Other Sources

For other sources (e.g., QuantsTower), use the `register_source_schema()` function (future feature).

## Error Handling

### Saturday Data
Saturday timestamps raise `ValueError` immediately:
```python
ValueError: Saturday trading data is invalid per session calendar. 
Timestamp: 1704585600 (2024-01-07 00:00:00-08:00), File: data.csv
```

### Conflicts
OHLCV conflicts are logged but don't stop ingestion. Check the return value:
```python
result = ingest_csv(...)
if result['conflicts'] > 0:
    for conflict in result['conflict_details']:
        print(f"Conflict at {conflict['timestamp']}: {conflict['reason']}")
```

## Example Workflows

### Workflow 1: Weekly Review

```python
# 1. Ingest week's data
for day in ["ES_Mon.csv", "ES_Tue.csv", "ES_Wed.csv", "ES_Thu.csv", "ES_Fri.csv"]:
    ingest_csv(day, "ES", "5m")

# 2. Review and annotate key days
save_day_annotation(
    symbol="ES",
    session_date="2024-01-10",
    content="Failed breakout, strong reversal",
    annotation_type="review",
    tags=["failed_breakout", "reversal"]
)

# 3. Query all annotations for the week
annotations = get_day_annotations(
    symbol="ES",
    start_date="2024-01-08",
    end_date="2024-01-12"
)
```

### Workflow 2: Data Quality Check

```python
# Ingest and check for conflicts
result = ingest_csv("data.csv", "ES", "5m")

if result['conflicts'] > 0:
    print(f"Warning: {result['conflicts']} conflicts detected")
    for conflict in result['conflict_details']:
        print(f"  Timestamp: {conflict['timestamp']}")
        print(f"  Existing: O={conflict['existing']['open']}, H={conflict['existing']['high']}")
        print(f"  New: O={conflict['new']['open']}, H={conflict['new']['high']}")
```

### Workflow 3: Update Annotation

```python
# Create initial observation
id1 = save_day_annotation(
    symbol="ES",
    session_date="2024-01-08",
    content="Possible trend day",
    annotation_type="observation"
)

# Later: Update with confirmation
id2 = save_day_annotation(
    symbol="ES",
    session_date="2024-01-08",
    content="Confirmed trend day - consistent directional movement",
    annotation_type="observation",
    supersedes_id=id1  # This marks id1 as 'superseded'
)
```

## Testing

See `example_usage.py` for a complete working example.

```bash
python example_usage.py
```

## Advanced Usage

### Direct SQL Access

For advanced queries, use SQLite directly:

```python
import sqlite3

conn = sqlite3.connect("market_data.db")
cursor = conn.cursor()

# Your custom query
cursor.execute("""
    SELECT td.session_date, COUNT(b.id) as bar_count
    FROM trade_days td
    LEFT JOIN bars b ON b.trade_day_id = td.id
    WHERE b.halt_period = 0
    GROUP BY td.session_date
    ORDER BY td.session_date
""")

for row in cursor.fetchall():
    print(row)

conn.close()
```

### Query Halt Period Data

```python
# Get bars including halt period
bars = get_bars(
    symbol="ES",
    session_date="2024-01-08",
    include_halt=True  # Include halt period bars
)

# Filter to only halt bars
halt_bars = [b for b in bars if b['halt_period']]
print(f"Found {len(halt_bars)} halt period bars")
```

## Limitations

- **No timezone conversion** - All input data must be in PT
- **No live data** - Designed for CSV exports only
- **TradingView default** - Other sources require schema registration (future)
- **SQLite only** - Not designed for high-frequency concurrent writes

## Design Philosophy

This tool is designed for:
- **Learning SQL** on real market data
- **Preserving observations** without hindsight bias
- **Building intuition** about relational data
- **Foundation** for more complex analytics

It is **not** designed for:
- Live trading
- Strategy backtesting
- High-frequency data
- Production trading systems

## Contributing

This is a learning tool. Keep it simple and readable.

## License

See repository license.
