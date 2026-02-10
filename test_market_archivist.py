"""
Tests for Market Data Archivist.

This file contains tests to verify all the requirements from the specification.
"""

import os
import sqlite3
import datetime
from zoneinfo import ZoneInfo
from market_archivist import (
    init_database,
    resolve_trade_day,
    is_saturday,
    is_halt_period,
    get_pt_datetime,
    ingest_csv,
    save_day_annotation,
    get_bars,
    get_day_annotations,
    get_trade_day,
    parse_tradingview_timestamp
)


PT_TIMEZONE = ZoneInfo("America/Los_Angeles")
TEST_DB = "test_market_data.db"


def cleanup_test_db():
    """Remove test database if it exists."""
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def test_trade_day_resolution():
    """Test trade day assignment rules."""
    print("\n=== Testing Trade Day Resolution ===")
    
    # Sunday 2024-01-07 3:00 PM PT → "2024-01-08" (Monday)
    dt = datetime.datetime(2024, 1, 7, 15, 0, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    result = resolve_trade_day(ts)
    assert result == "2024-01-08", f"Expected 2024-01-08, got {result}"
    print(f"✓ Sunday 3:00 PM PT → {result}")
    
    # Monday 2024-01-08 1:00 PM PT → "2024-01-08"
    dt = datetime.datetime(2024, 1, 8, 13, 0, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    result = resolve_trade_day(ts)
    assert result == "2024-01-08", f"Expected 2024-01-08, got {result}"
    print(f"✓ Monday 1:00 PM PT → {result}")
    
    # Monday 2024-01-08 2:30 PM PT → None (halt)
    dt = datetime.datetime(2024, 1, 8, 14, 30, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    result = resolve_trade_day(ts)
    assert result is None, f"Expected None (halt), got {result}"
    print(f"✓ Monday 2:30 PM PT → None (halt)")
    
    # Friday 2024-01-12 1:00 PM PT → "2024-01-12"
    dt = datetime.datetime(2024, 1, 12, 13, 0, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    result = resolve_trade_day(ts)
    assert result == "2024-01-12", f"Expected 2024-01-12, got {result}"
    print(f"✓ Friday 1:00 PM PT → {result}")
    
    # Saturday should raise ValueError
    dt = datetime.datetime(2024, 1, 13, 12, 0, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    try:
        result = resolve_trade_day(ts)
        assert False, "Should have raised ValueError for Saturday"
    except ValueError as e:
        assert "Saturday trading data is invalid" in str(e)
        print(f"✓ Saturday raises ValueError: {e}")


def test_halt_detection():
    """Test halt period detection."""
    print("\n=== Testing Halt Detection ===")
    
    # 2:00 PM PT (14:00) - should be halt
    dt = datetime.datetime(2024, 1, 8, 14, 0, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    assert is_halt_period(ts), "14:00 should be halt period"
    print(f"✓ 2:00 PM PT is halt period")
    
    # 2:59 PM PT (14:59) - should be halt
    dt = datetime.datetime(2024, 1, 8, 14, 59, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    assert is_halt_period(ts), "14:59 should be halt period"
    print(f"✓ 2:59 PM PT is halt period")
    
    # 3:00 PM PT (15:00) - should NOT be halt
    dt = datetime.datetime(2024, 1, 8, 15, 0, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    assert not is_halt_period(ts), "15:00 should not be halt period"
    print(f"✓ 3:00 PM PT is not halt period")
    
    # 1:59 PM PT (13:59) - should NOT be halt
    dt = datetime.datetime(2024, 1, 8, 13, 59, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    assert not is_halt_period(ts), "13:59 should not be halt period"
    print(f"✓ 1:59 PM PT is not halt period")


def test_saturday_detection():
    """Test Saturday detection."""
    print("\n=== Testing Saturday Detection ===")
    
    # Saturday
    dt = datetime.datetime(2024, 1, 13, 12, 0, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    assert is_saturday(ts), "Should detect Saturday"
    print(f"✓ Saturday detected")
    
    # Sunday (not Saturday)
    dt = datetime.datetime(2024, 1, 14, 12, 0, 0, tzinfo=PT_TIMEZONE)
    ts = int(dt.timestamp())
    assert not is_saturday(ts), "Sunday should not be Saturday"
    print(f"✓ Sunday not detected as Saturday")


def test_database_initialization():
    """Test database initialization."""
    print("\n=== Testing Database Initialization ===")
    
    cleanup_test_db()
    
    # Initialize database
    init_database(TEST_DB)
    assert os.path.exists(TEST_DB), "Database file should exist"
    print(f"✓ Database file created")
    
    # Check tables exist
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    assert "trade_days" in tables, "trade_days table should exist"
    assert "bars" in tables, "bars table should exist"
    assert "day_annotations" in tables, "day_annotations table should exist"
    print(f"✓ All tables created: {tables}")
    
    conn.close()
    
    # Test idempotency (should not error when called again)
    init_database(TEST_DB)
    print(f"✓ Database initialization is idempotent")


def test_csv_ingestion_and_idempotency():
    """Test CSV ingestion and idempotency."""
    print("\n=== Testing CSV Ingestion ===")
    
    cleanup_test_db()
    init_database(TEST_DB)
    
    # Ingest CSV
    result = ingest_csv(
        file_path="TradingView-Feb9-CME_MINI_MNQ1!, 1_5cedc.csv",
        symbol="MNQ",
        timeframe="1m",
        source="tradingview",
        db_path=TEST_DB
    )
    
    assert result["inserted"] > 0, "Should have inserted bars"
    assert result["skipped"] == 0, "First ingestion should skip nothing"
    assert result["conflicts"] == 0, "First ingestion should have no conflicts"
    print(f"✓ First ingestion: {result['inserted']} bars inserted")
    
    # Ingest same file again (test idempotency)
    result2 = ingest_csv(
        file_path="TradingView-Feb9-CME_MINI_MNQ1!, 1_5cedc.csv",
        symbol="MNQ",
        timeframe="1m",
        source="tradingview",
        db_path=TEST_DB
    )
    
    assert result2["inserted"] == 0, "Second ingestion should insert nothing"
    assert result2["skipped"] == result["inserted"], "Should skip all previously inserted bars"
    print(f"✓ Second ingestion (idempotent): {result2['skipped']} bars skipped")


def test_annotations():
    """Test annotation saving and querying."""
    print("\n=== Testing Annotations ===")
    
    cleanup_test_db()
    init_database(TEST_DB)
    
    # Ingest some data first
    ingest_csv(
        file_path="TradingView-Feb9-CME_MINI_MNQ1!, 1_5cedc.csv",
        symbol="MNQ",
        timeframe="1m",
        source="tradingview",
        db_path=TEST_DB
    )
    
    # Get a session date
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT session_date FROM trade_days ORDER BY session_date LIMIT 1")
    session_date = cursor.fetchone()[0]
    conn.close()
    
    # Save annotation
    ann_id = save_day_annotation(
        symbol="MNQ",
        session_date=session_date,
        content="Test observation",
        annotation_type="observation",
        tags=["test", "breakout"],
        db_path=TEST_DB
    )
    
    assert ann_id > 0, "Should return annotation ID"
    print(f"✓ Annotation saved with ID: {ann_id}")
    
    # Query annotations
    annotations = get_day_annotations(
        symbol="MNQ",
        start_date=session_date,
        end_date=session_date,
        db_path=TEST_DB
    )
    
    assert len(annotations) == 1, "Should find 1 annotation"
    assert annotations[0]["content"] == "Test observation"
    assert "test" in annotations[0]["tags"]
    print(f"✓ Annotation retrieved successfully")
    
    # Test superseding
    ann_id2 = save_day_annotation(
        symbol="MNQ",
        session_date=session_date,
        content="Updated observation",
        annotation_type="observation",
        tags=["test", "updated"],
        supersedes_id=ann_id,
        db_path=TEST_DB
    )
    
    # Query active annotations (should only get the new one)
    annotations = get_day_annotations(
        symbol="MNQ",
        start_date=session_date,
        end_date=session_date,
        status="active",
        db_path=TEST_DB
    )
    
    assert len(annotations) == 1, "Should find 1 active annotation"
    assert annotations[0]["id"] == ann_id2, "Should be the new annotation"
    assert annotations[0]["content"] == "Updated observation"
    print(f"✓ Annotation superseding works correctly")
    
    # Query superseded annotations
    annotations = get_day_annotations(
        symbol="MNQ",
        start_date=session_date,
        end_date=session_date,
        status="superseded",
        db_path=TEST_DB
    )
    
    assert len(annotations) == 1, "Should find 1 superseded annotation"
    assert annotations[0]["id"] == ann_id, "Should be the old annotation"
    print(f"✓ Superseded annotations queryable")


def test_bar_queries():
    """Test bar querying."""
    print("\n=== Testing Bar Queries ===")
    
    cleanup_test_db()
    init_database(TEST_DB)
    
    # Ingest data
    ingest_csv(
        file_path="TradingView-Feb9-CME_MINI_MNQ1!, 1_5cedc.csv",
        symbol="MNQ",
        timeframe="1m",
        source="tradingview",
        db_path=TEST_DB
    )
    
    # Get a session date
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT session_date FROM trade_days ORDER BY session_date LIMIT 1")
    session_date = cursor.fetchone()[0]
    conn.close()
    
    # Query bars (default excludes halt)
    bars = get_bars(
        symbol="MNQ",
        session_date=session_date,
        db_path=TEST_DB
    )
    
    assert len(bars) > 0, "Should find bars"
    assert all(not bar["halt_period"] for bar in bars), "Should exclude halt period bars"
    print(f"✓ Retrieved {len(bars)} non-halt bars for {session_date}")
    
    # Query bars including halt
    bars_with_halt = get_bars(
        symbol="MNQ",
        session_date=session_date,
        include_halt=True,
        db_path=TEST_DB
    )
    
    assert len(bars_with_halt) >= len(bars), "Including halt should return same or more bars"
    print(f"✓ Retrieved {len(bars_with_halt)} bars (including halt)")


def test_trade_day_query():
    """Test trade day querying."""
    print("\n=== Testing Trade Day Query ===")
    
    cleanup_test_db()
    init_database(TEST_DB)
    
    # Ingest data
    ingest_csv(
        file_path="TradingView-Feb9-CME_MINI_MNQ1!, 1_5cedc.csv",
        symbol="MNQ",
        timeframe="1m",
        source="tradingview",
        db_path=TEST_DB
    )
    
    # Get a session date
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT session_date FROM trade_days ORDER BY session_date LIMIT 1")
    session_date = cursor.fetchone()[0]
    conn.close()
    
    # Query trade day
    trade_day = get_trade_day(
        symbol="MNQ",
        session_date=session_date,
        db_path=TEST_DB
    )
    
    assert trade_day is not None, "Should find trade day"
    assert trade_day["symbol"] == "MNQ"
    assert trade_day["session_date"] == session_date
    print(f"✓ Trade day retrieved: {trade_day}")
    
    # Query non-existent trade day
    trade_day = get_trade_day(
        symbol="MNQ",
        session_date="2020-01-01",
        db_path=TEST_DB
    )
    
    assert trade_day is None, "Should return None for non-existent trade day"
    print(f"✓ Non-existent trade day returns None")


def test_timestamp_parsing():
    """Test timestamp parsing."""
    print("\n=== Testing Timestamp Parsing ===")
    
    # Test ISO 8601 format with timezone
    ts = parse_tradingview_timestamp("2026-02-06T03:16:00-08:00")
    dt = get_pt_datetime(ts)
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 6
    assert dt.hour == 3
    assert dt.minute == 16
    print(f"✓ ISO 8601 format parsed: {dt}")
    
    # Test simple format without timezone
    ts = parse_tradingview_timestamp("2024-01-08 13:30:00")
    dt = get_pt_datetime(ts)
    assert dt.year == 2024
    assert dt.month == 1
    assert dt.day == 8
    assert dt.hour == 13
    assert dt.minute == 30
    print(f"✓ Simple format parsed: {dt}")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("Running Market Data Archivist Tests")
    print("=" * 60)
    
    try:
        test_trade_day_resolution()
        test_halt_detection()
        test_saturday_detection()
        test_database_initialization()
        test_timestamp_parsing()
        test_csv_ingestion_and_idempotency()
        test_annotations()
        test_bar_queries()
        test_trade_day_query()
        
        print("\n" + "=" * 60)
        print("✅ All tests passed!")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise
    finally:
        cleanup_test_db()


if __name__ == "__main__":
    run_all_tests()
