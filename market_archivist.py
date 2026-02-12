"""
Market Data Archivist - SQLite-based market data storage and annotation system.

This module provides functions to ingest CSV market data, normalize it,
and store it in a SQLite database with trade day resolution and annotations.
"""

import sqlite3
import csv
import json
import datetime
from zoneinfo import ZoneInfo
from typing import Optional


# Timezone constants
PT_TIMEZONE = ZoneInfo("America/Los_Angeles")


def get_pt_datetime(timestamp: int) -> datetime.datetime:
    """Convert Unix timestamp to PT datetime."""
    return datetime.datetime.fromtimestamp(timestamp, tz=PT_TIMEZONE)


def is_saturday(timestamp: int) -> bool:
    """Check if timestamp falls on Saturday in PT."""
    dt = get_pt_datetime(timestamp)
    return dt.weekday() == 5  # 5 = Saturday


def is_halt_period(timestamp: int) -> bool:
    """
    Check if timestamp falls in daily halt (2-3 PM PT).
    
    Returns True if hour is 14 (2 PM) or any time before 3 PM within that hour.
    """
    dt = get_pt_datetime(timestamp)
    return dt.hour == 14  # 14:00 - 14:59 is the halt period


def resolve_trade_day(timestamp: int) -> Optional[str]:
    """
    Given a Unix timestamp (in PT), return the trade day (YYYY-MM-DD).
    
    Rules:
        - Timestamps >= 3:00 PM PT → next calendar day
        - Timestamps < 2:00 PM PT → current calendar day
        - Timestamps in halt (2-3 PM PT) → return None
        - Saturday → raise ValueError
    
    Returns:
        - Trade day string (YYYY-MM-DD) or None for halt period
    
    Examples:
        - Sunday 2024-01-07 3:00 PM PT → "2024-01-08"
        - Monday 2024-01-08 1:00 PM PT → "2024-01-08"
        - Monday 2024-01-08 2:30 PM PT → None (halt)
    """
    if is_saturday(timestamp):
        dt = get_pt_datetime(timestamp)
        raise ValueError(
            f"Saturday trading data is invalid per session calendar. "
            f"Timestamp: {timestamp} ({dt})"
        )
    
    if is_halt_period(timestamp):
        return None
    
    dt = get_pt_datetime(timestamp)
    
    # If >= 15:00 (3 PM), assign to next calendar day
    if dt.hour >= 15:
        next_day = dt.date() + datetime.timedelta(days=1)
        return next_day.strftime("%Y-%m-%d")
    else:
        # < 14:00 (2 PM), assign to current calendar day
        return dt.date().strftime("%Y-%m-%d")


def init_database(db_path: str = "market_data.db") -> None:
    """
    Creates the database and tables if they don't exist.
    Safe to call repeatedly (idempotent).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create trade_days table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_days (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            session_date TEXT,
            source TEXT,
            UNIQUE(symbol, session_date, source)
        )
    """)
    
    # Create bars table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bars (
            id INTEGER PRIMARY KEY,
            trade_day_id INTEGER,
            timestamp INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            halt_period INTEGER,
            raw_json TEXT,
            FOREIGN KEY(trade_day_id) REFERENCES trade_days(id)
        )
    """)
    
    # Create day_annotations table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS day_annotations (
            id INTEGER PRIMARY KEY,
            trade_day_id INTEGER,
            annotation_type TEXT,
            content TEXT,
            tags TEXT,
            source TEXT,
            created_at INTEGER,
            supersedes_id INTEGER,
            status TEXT DEFAULT 'active',
            FOREIGN KEY(trade_day_id) REFERENCES trade_days(id),
            FOREIGN KEY(supersedes_id) REFERENCES day_annotations(id)
        )
    """)
    
    conn.commit()
    conn.close()


def get_or_create_trade_day(
    symbol: str,
    session_date: str,
    source: str,
    cursor: sqlite3.Cursor
) -> int:
    """
    Gets trade_day_id, creating the record if it doesn't exist.
    
    Args:
        cursor: Database cursor to use for the operation
    
    Returns:
        The trade_day.id
    """
    # Try to find existing trade day
    cursor.execute(
        "SELECT id FROM trade_days WHERE symbol = ? AND session_date = ? AND source = ?",
        (symbol, session_date, source)
    )
    result = cursor.fetchone()
    
    if result:
        trade_day_id = result[0]
    else:
        # Create new trade day
        cursor.execute(
            "INSERT INTO trade_days (symbol, session_date, source) VALUES (?, ?, ?)",
            (symbol, session_date, source)
        )
        trade_day_id = cursor.lastrowid
    
    return trade_day_id


def parse_tradingview_timestamp(time_str: str) -> int:
    """
    Parse TradingView timestamp string to Unix epoch seconds.
    
    TradingView format: "YYYY-MM-DDTHH:MM:SS-HH:MM" (ISO 8601 with timezone offset)
    or "YYYY-MM-DD HH:MM:SS" (without timezone)
    
    Returns Unix timestamp in seconds.
    """
    # Try ISO 8601 format first (with timezone)
    if 'T' in time_str:
        # Parse with timezone info
        dt = datetime.datetime.fromisoformat(time_str)
        # Convert to PT if not already
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=PT_TIMEZONE)
        else:
            dt = dt.astimezone(PT_TIMEZONE)
        return int(dt.timestamp())
    else:
        # Parse without timezone, assume PT
        dt = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=PT_TIMEZONE)
        return int(dt.timestamp())


def ingest_csv(
    file_path: str,
    symbol: str,
    timeframe: str,
    source: str = "tradingview",
    db_path: str = "market_data.db"
) -> dict:
    """
    Ingests a CSV file of market data into the database.
    
    Returns:
        {
            "inserted": N,
            "skipped": M,
            "conflicts": K,
            "conflict_details": [...]
        }
    
    Behavior:
        - Reads CSV and validates structure
        - For each bar, determine trade_day using assignment rules
        - Check if bar already exists (trade_day_id, timestamp)
        - If exact match: skip
        - If conflict (different OHLCV): log warning and skip
        - If new: insert
        - Flag bars in halt period (2-3 PM PT) with halt_period=1
        - Raise ValueError for Saturday data
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    stats = {
        "inserted": 0,
        "skipped": 0,
        "conflicts": 0,
        "conflict_details": []
    }
    
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Parse timestamp based on source
            if source == "tradingview":
                timestamp = parse_tradingview_timestamp(row['time'])
            else:
                # For other sources, assume "timestamp" column in format "YYYY-MM-DD HH:MM:SS"
                time_str = row.get('timestamp', row.get('time'))
                dt = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=PT_TIMEZONE)
                timestamp = int(dt.timestamp())
            
            # Parse OHLCV
            open_price = float(row['open'])
            high_price = float(row['high'])
            low_price = float(row['low'])
            close_price = float(row['close'])
            
            # Handle empty volume
            volume_str = row.get('volume', row.get('Volume', '0')).strip()
            volume = float(volume_str) if volume_str else 0.0
            
            # Resolve trade day (may raise ValueError for Saturday)
            try:
                session_date = resolve_trade_day(timestamp)
            except ValueError as e:
                # Re-raise with file path context
                raise ValueError(str(e) + f", File: {file_path}")
            
            # Determine if this is a halt period bar
            halt_period = 1 if session_date is None else 0
            
            # For halt period bars, we still store them but with trade_day_id = NULL
            if halt_period:
                trade_day_id = None
            else:
                trade_day_id = get_or_create_trade_day(symbol, session_date, source, cursor)
            
            # Create raw JSON representation
            raw_json = json.dumps({
                "timestamp": timestamp,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "source_row": row
            })
            
            # Check for existing bar
            if halt_period:
                cursor.execute(
                    "SELECT id, open, high, low, close, volume FROM bars WHERE timestamp = ? AND halt_period = 1",
                    (timestamp,)
                )
            else:
                cursor.execute(
                    "SELECT id, open, high, low, close, volume FROM bars WHERE trade_day_id = ? AND timestamp = ?",
                    (trade_day_id, timestamp)
                )
            
            existing = cursor.fetchone()
            
            if existing:
                # Check if OHLCV matches
                existing_id, existing_open, existing_high, existing_low, existing_close, existing_volume = existing
                
                if (abs(existing_open - open_price) < 0.001 and
                    abs(existing_high - high_price) < 0.001 and
                    abs(existing_low - low_price) < 0.001 and
                    abs(existing_close - close_price) < 0.001 and
                    abs(existing_volume - volume) < 0.001):
                    # Exact match, skip
                    stats["skipped"] += 1
                else:
                    # Conflict
                    stats["conflicts"] += 1
                    stats["conflict_details"].append({
                        "timestamp": timestamp,
                        "reason": "OHLCV mismatch",
                        "file": file_path,
                        "existing": {
                            "open": existing_open,
                            "high": existing_high,
                            "low": existing_low,
                            "close": existing_close,
                            "volume": existing_volume
                        },
                        "new": {
                            "open": open_price,
                            "high": high_price,
                            "low": low_price,
                            "close": close_price,
                            "volume": volume
                        }
                    })
            else:
                # Insert new bar
                cursor.execute(
                    """INSERT INTO bars 
                       (trade_day_id, timestamp, open, high, low, close, volume, halt_period, raw_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (trade_day_id, timestamp, open_price, high_price, low_price, close_price, volume, halt_period, raw_json)
                )
                stats["inserted"] += 1
    
    conn.commit()
    conn.close()
    
    return stats


def save_day_annotation(
    symbol: str,
    session_date: str,
    content: str,
    annotation_type: str = "observation",
    tags: Optional[list[str]] = None,
    source: str = "manual",
    supersedes_id: Optional[int] = None,
    db_path: str = "market_data.db"
) -> int:
    """
    Saves an annotation for a specific trade day.
    
    Returns:
        The ID of the newly created annotation.
    
    Behavior:
        - Find or create trade_day record
        - Insert annotation
        - If supersedes_id provided, mark old annotation as 'superseded'
        - Store tags as JSON string
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get or create trade day (assuming tradingview source by default)
    trade_day_id = get_or_create_trade_day(symbol, session_date, "tradingview", cursor)
    
    # Convert tags to JSON
    tags_json = json.dumps(tags if tags else [])
    
    # Get current timestamp
    created_at = int(datetime.datetime.now(tz=PT_TIMEZONE).timestamp())
    
    # Insert annotation
    cursor.execute(
        """INSERT INTO day_annotations 
           (trade_day_id, annotation_type, content, tags, source, created_at, supersedes_id, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'active')""",
        (trade_day_id, annotation_type, content, tags_json, source, created_at, supersedes_id)
    )
    
    annotation_id = cursor.lastrowid
    
    # If supersedes another annotation, mark the old one as superseded
    if supersedes_id:
        cursor.execute(
            "UPDATE day_annotations SET status = 'superseded' WHERE id = ?",
            (supersedes_id,)
        )
    
    conn.commit()
    conn.close()
    
    return annotation_id


def get_bars(
    symbol: str,
    session_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    timeframe: Optional[str] = None,
    include_halt: bool = False,
    source: str = "tradingview",
    db_path: str = "market_data.db"
) -> list[dict]:
    """
    Queries bars from the database.
    
    Returns:
        List of bar dictionaries with all fields.
    
    Behavior:
        - Default: filters WHERE halt_period = 0
        - Joins with trade_days to include session_date in results
        - Can query single day or date range
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Build query
    query = """
        SELECT 
            b.id,
            b.timestamp,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume,
            b.halt_period,
            b.raw_json,
            td.session_date
        FROM bars b
        LEFT JOIN trade_days td ON b.trade_day_id = td.id
        WHERE td.symbol = ? AND td.source = ?
    """
    params = [symbol, source]
    
    # Add date filters
    if session_date:
        query += " AND td.session_date = ?"
        params.append(session_date)
    elif start_date and end_date:
        query += " AND td.session_date >= ? AND td.session_date <= ?"
        params.extend([start_date, end_date])
    
    # Add halt filter
    if not include_halt:
        query += " AND b.halt_period = 0"
    
    query += " ORDER BY b.timestamp"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    conn.close()
    
    # Convert to list of dictionaries
    result = []
    for row in rows:
        result.append({
            "id": row["id"],
            "timestamp": row["timestamp"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "halt_period": bool(row["halt_period"]),
            "session_date": row["session_date"],
            "raw_json": row["raw_json"]
        })
    
    return result


def get_day_annotations(
    symbol: str,
    start_date: str,
    end_date: str,
    tags: Optional[list[str]] = None,
    status: str = "active",
    annotation_type: Optional[str] = None,
    db_path: str = "market_data.db"
) -> list[dict]:
    """
    Queries annotations for a date range.
    
    Returns:
        List of annotation dictionaries.
    
    Behavior:
        - Default: only returns status='active'
        - If tags provided, filter to annotations containing ANY of the tags
        - Parse JSON tags field for filtering
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Build query
    query = """
        SELECT 
            da.id,
            da.annotation_type,
            da.content,
            da.tags,
            da.source,
            da.created_at,
            da.supersedes_id,
            da.status,
            td.session_date
        FROM day_annotations da
        JOIN trade_days td ON da.trade_day_id = td.id
        WHERE td.symbol = ?
          AND td.session_date >= ?
          AND td.session_date <= ?
    """
    params = [symbol, start_date, end_date]
    
    # Add status filter
    if status != "all":
        query += " AND da.status = ?"
        params.append(status)
    
    # Add annotation type filter
    if annotation_type:
        query += " AND da.annotation_type = ?"
        params.append(annotation_type)
    
    query += " ORDER BY td.session_date, da.created_at"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    conn.close()
    
    # Convert to list of dictionaries and filter by tags if needed
    result = []
    for row in rows:
        row_tags = json.loads(row["tags"]) if row["tags"] else []
        
        # If tags filter is provided, check if any tag matches
        if tags:
            if not any(tag in row_tags for tag in tags):
                continue
        
        result.append({
            "id": row["id"],
            "session_date": row["session_date"],
            "annotation_type": row["annotation_type"],
            "content": row["content"],
            "tags": row_tags,
            "source": row["source"],
            "created_at": row["created_at"],
            "supersedes_id": row["supersedes_id"],
            "status": row["status"]
        })
    
    return result


def get_trade_day(
    symbol: str,
    session_date: str,
    source: str = "tradingview",
    db_path: str = "market_data.db"
) -> Optional[dict]:
    """
    Gets a trade_day record.
    
    Returns:
        Dictionary with trade_day fields or None if not found.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT id, symbol, session_date, source FROM trade_days WHERE symbol = ? AND session_date = ? AND source = ?",
        (symbol, session_date, source)
    )
    
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return {
            "id": row["id"],
            "symbol": row["symbol"],
            "session_date": row["session_date"],
            "source": row["source"]
        }
    
    return None


def register_source_schema(
    source: str,
    example_files: list[str],
    column_map: Optional[dict[str, str]] = None,
    timestamp_format: Optional[str] = None,
    db_path: str = "market_data.db"
) -> None:
    """
    (FUTURE) Learns schema from example files and stores mapping.
    
    For initial implementation, only support 'tradingview' source.
    Raise NotImplementedError for other sources.
    """
    if source != "tradingview":
        raise NotImplementedError(
            f"Schema registration for source '{source}' is not yet implemented. "
            f"Currently only 'tradingview' is supported by default."
        )
