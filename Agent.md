# Agent: Market Data Archivist

## Purpose

This agent ingests CSV exports from TradingView (default) and other sources
(e.g., QuantsTower), normalizes market data, and stores it in a local SQLite3
database. It also persists trade-day–level annotations created during
analysis so insights do not need to be re-created or copied between scripts.

The agent acts as a durable market memory, not an analysis engine.

## Core Principles

- Python-first execution
- Append-only persistence
- Idempotent ingestion
- Trade day is the unit of reasoning
- Human annotations are first-class data
- Schemas are learned, not hard-coded

## Execution Model

The agent is designed to be invoked repeatedly from Python scripts.

Each run may:
- Ingest new CSV data
- Append annotations

No invocation mutates or deletes existing records.

---

## Session Calendar (Authoritative)

All session logic is defined in **Pacific Time (PT / America/Los_Angeles)**.

### Trading Week

- **Opens:** Sunday 3:00 PM PT
- **Runs continuously** with a daily halt (see below)
- **Closes:** Friday 2:00 PM PT
- **No trading on:** Saturday

### Daily Halt

- **Time:** 2:00 PM – 3:00 PM PT (every day except Friday)
- **Friday exception:** The trading week ends at Friday 2:00 PM PT. There is no halt on Friday; the session simply closes and the week ends.

---

## Trade Day Definition

A **trade day** is defined as:

> The calendar date (YYYY-MM-DD, PT) of the session that begins at 3:00 PM PT.

### Trade Day Assignment Rules

- Timestamps **>= 3:00 PM PT** belong to the **next calendar day's trade day**
- Timestamps **< 2:00 PM PT** belong to the **current calendar day's trade day**
- Timestamps between **2:00 PM – 3:00 PM PT** fall in the **daily halt**
- **Saturday timestamps are invalid** (raise error)

### Trade Day Assignment Examples

#### Corner Cases

```
Sunday 2024-01-07 3:00 PM PT    → trade_day = "2024-01-08" (Monday)
Sunday 2024-01-07 4:30 PM PT    → trade_day = "2024-01-08" (Monday)
Monday 2024-01-08 1:00 PM PT    → trade_day = "2024-01-08" (Monday)
Monday 2024-01-08 2:00 PM PT    → HALT (not assigned to any trade day)
Monday 2024-01-08 3:00 PM PT    → trade_day = "2024-01-09" (Tuesday)

Friday 2024-01-12 1:00 PM PT    → trade_day = "2024-01-12" (Friday)
Friday 2024-01-12 1:59 PM PT    → trade_day = "2024-01-12" (Friday)
Friday 2024-01-12 2:00 PM PT    → MARKET CLOSED (week ends)
Friday 2024-01-12 3:00 PM PT    → INVALID (market closed)

Saturday 2024-01-13 (any time)  → INVALID (raise ValueError)
```

#### Session Composition Example

Trade Day **"2024-01-12" (Friday)** contains bars from:
- **Start:** Thursday 2024-01-11 3:00 PM PT
- **End:** Friday 2024-01-12 2:00 PM PT (exclusive)
- **Duration:** 23 hours

Trade Day **"2024-01-08" (Monday)** contains bars from:
- **Start:** Sunday 2024-01-07 3:00 PM PT  
- **End:** Monday 2024-01-08 2:00 PM PT (exclusive)
- **Duration:** 23 hours

---

## Halt-Spanning Data Handling

Data that falls within the daily halt is not discarded silently.

### Halt Rules

Bars between **2:00 PM – 3:00 PM PT** are:
- Flagged as `halt_period = 1` (true)
- Stored separately
- Excluded from normal analytics and joins
- Never assigned to a trade day

### Query Behavior

- **Default bar queries** automatically filter `WHERE halt_period = 0`
- To include halt data, explicitly pass `include_halt=True`
- Example: `get_bars(symbol, date, include_halt=False)  # default`

### Halt Data Preservation

Halt data is preserved for:
- Diagnostics
- Vendor validation
- Data quality checks

---

## Timezone Handling

### Input Data Requirements

**All CSV input data must be in Pacific Time (PT / America/Los_Angeles).**

- The agent does not perform timezone conversion from other zones
- Source systems must provide data in PT
- Be mindful of daylight saving time transitions
- If source data is in another timezone, convert to PT before ingestion

**Rationale:** Timezone conversion complexity is kept out of the agent. Data providers are responsible for delivering PT-normalized timestamps.

---

## Database Schema

### Trade Days (Join Anchor)

```sql
CREATE TABLE trade_days (
    id INTEGER PRIMARY KEY,
    symbol TEXT,
    session_date TEXT,        -- YYYY-MM-DD (PT trade day)
    source TEXT,
    UNIQUE(symbol, session_date, source)
);
```

### Bars (Normalized Market Data)

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

### Trade-Day Annotations

```sql
CREATE TABLE day_annotations (
    id INTEGER PRIMARY KEY,
    trade_day_id INTEGER,
    annotation_type TEXT,     -- observation | hypothesis | review
    content TEXT,
    tags TEXT,
    source TEXT,              -- manual | script
    created_at INTEGER,
    supersedes_id INTEGER,    -- references another annotation if this replaces it
    status TEXT DEFAULT 'active',  -- active | superseded | deprecated
    FOREIGN KEY(trade_day_id) REFERENCES trade_days(id),
    FOREIGN KEY(supersedes_id) REFERENCES day_annotations(id)
);
```

**Annotation Properties:**
- One-to-many per trade day
- Append-only (but can be marked as superseded)
- Time-agnostic within the session

**Annotation Versioning:**
- To correct an annotation, create a new one with `supersedes_id` pointing to the old one
- The old annotation's `status` is automatically updated to 'superseded'
- This preserves history while marking current state

---

## Source Handling

### Default Source: TradingView

- TradingView CSVs are assumed by default
- Treated as the canonical schema

### Exception Sources

- QuantsTower
- Future vendors

Each source is normalized into the same internal bar schema.

---

## Schema Detection & Learning

The agent supports schema inference via example files.

### Schema Registration

```python
register_source_schema(
    source: str,
    example_files: list[str],
    column_map: dict[str, str] = None,
    timestamp_format: str = None
)
```

### Schema Requirements

The agent must unambiguously identify:

1. **Primary timestamp column** (bar start or bar close time)
2. **Open, High, Low, Close columns**
3. **Volume column** (optional - set to NULL if missing)

### Behavior

- **First ingestion:** The agent learns the schema from the provided example files
- **Column mapping:** If column names are ambiguous, provide explicit `column_map`
- **Subsequent ingestions:** Column structure must not change
- **Schema validation:** On every ingestion, the agent validates that the CSV structure matches the learned schema

### Example: Custom Source

```python
register_source_schema(
    source="quantstower",
    example_files=["sample.csv"],
    column_map={
        "timestamp": "DateTime",
        "open": "O",
        "high": "H",
        "low": "L",
        "close": "C",
        "volume": "Vol"
    },
    timestamp_format="%Y-%m-%d %H:%M:%S"
)
```

### Example: TradingView (Default)

```python
# TradingView schema is built-in, no registration needed
# Expects columns: time, open, high, low, close, volume
```

---

## Idempotency & Duplicate Handling

### Strategy

On ingestion, for each bar:

1. Check for existing bars matching `(trade_day_id, timestamp)`
2. **If exact match exists** (same OHLCV values):
   - Skip silently
3. **If conflict exists** (different OHLCV for same timestamp):
   - Log warning with details
   - Skip (do not overwrite)
4. **If new:** Insert

### Return Summary

```python
{
    "inserted": N,
    "skipped": M,
    "conflicts": K,
    "conflict_details": [
        {"timestamp": ..., "reason": "OHLCV mismatch", "file": ...}
    ]
}
```

**Rationale:** Prevents accidental data corruption while allowing safe re-runs.

---

## Error Handling

### Saturday Data

```python
# Raises ValueError with details
raise ValueError(
    f"Saturday trading data is invalid per session calendar. "
    f"Timestamp: {timestamp}, File: {file_path}"
)
```

**Rationale:** Saturday data indicates upstream error. Do not silently discard.

### Invalid Timestamps

- Timestamps outside trading hours (except halt) generate warnings but may be stored with a flag for investigation
- Malformed timestamps raise `ValueError`

---

## Python Command Interface

### Ingest CSV

```python
ingest_csv(
    file_path: str,
    symbol: str,
    timeframe: str,
    source: str = "tradingview"
) -> dict
```

**Behavior:**
- Uses registered schema or TradingView defaults
- Validates CSV structure against learned schema
- Normalizes timestamps to PT (must already be in PT)
- Resolves trade day using assignment rules
- Flags halt-period rows
- Safe to run repeatedly (idempotent)
- Returns ingestion summary (see Idempotency section)

**Raises:**
- `ValueError` for Saturday data
- `ValueError` for schema mismatch
- `SchemaNotFoundError` if source not registered

### Save Trade-Day Annotation

```python
save_day_annotation(
    symbol: str,
    session_date: str,        # YYYY-MM-DD (trade day)
    content: str,
    annotation_type: str = "observation",
    tags: list[str] | None = None,
    source: str = "manual",
    supersedes_id: int | None = None  # ID of annotation to replace
) -> int  # returns new annotation ID
```

**Behavior:**
- If `supersedes_id` is provided, marks that annotation as 'superseded'
- Returns the ID of the newly created annotation
- Append-only (old annotations are never deleted)

### Query Bars

```python
get_bars(
    symbol: str,
    session_date: str | None = None,     # Single trade day
    start_date: str | None = None,        # Date range (inclusive)
    end_date: str | None = None,
    timeframe: str | None = None,
    include_halt: bool = False,
    source: str = "tradingview"
) -> list[dict]
```

**Behavior:**
- Returns bars as list of dictionaries
- Default: filters out halt_period bars (WHERE halt_period = 0)
- Date parameters accept YYYY-MM-DD format (trade days, not wall clock dates)
- Can query single day or range

**Returns:**
```python
[
    {
        "timestamp": 1704675600,  # epoch seconds
        "open": 100.5,
        "high": 101.2,
        "low": 100.1,
        "close": 101.0,
        "volume": 1500000,
        "halt_period": False,
        "session_date": "2024-01-08"
    },
    ...
]
```

---

## Query Patterns

### Get Trade Day

```python
get_trade_day(
    symbol: str,
    session_date: str
) -> dict | None
```

Returns trade day record or None if not found.

### Get Day Annotations

```python
get_day_annotations(
    symbol: str,
    start_date: str,
    end_date: str,
    tags: list[str] | None = None,
    status: str = "active",  # active | superseded | deprecated | all
    annotation_type: str | None = None
) -> list[dict]
```

**Behavior:**
- Returns annotations for trade days in range
- Default: only returns `status = 'active'` annotations
- Can filter by tags and annotation_type

**Returns:**
```python
[
    {
        "id": 42,
        "session_date": "2024-01-08",
        "annotation_type": "observation",
        "content": "Strong momentum break above key level",
        "tags": ["momentum", "breakout"],
        "source": "manual",
        "created_at": 1704675600,
        "supersedes_id": None,
        "status": "active"
    },
    ...
]
```

---

## Data Guarantees

- No silent data loss
- Deterministic trade day resolution
- Repeatable ingestion (idempotent)
- Clear separation between:
  - Trading data
  - Halt data
  - Human reasoning
- Saturday data raises errors (never silently accepted)
- Duplicate detection and conflict reporting

---

## Out of Scope

- Live data ingestion
- Strategy execution
- Signal generation
- Charting
- Backtesting
- Timezone conversion (data must arrive in PT)

---

## Design Intent

This agent exists to:

- **Preserve what happened**
- **Preserve what you thought**
- **Preserve when you thought it**

Without hindsight pollution.
