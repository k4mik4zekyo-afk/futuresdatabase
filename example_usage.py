"""
Example usage of the Market Data Archivist.

This script demonstrates the basic workflows for ingesting CSV data,
adding annotations, and querying the database.
"""

from market_archivist import (
    init_database,
    ingest_csv,
    save_day_annotation,
    get_bars,
    get_day_annotations,
    get_trade_day
)
import sqlite3


def main():
    print("=" * 60)
    print("Market Data Archivist - Example Usage")
    print("=" * 60)
    
    # 1. Initialize database
    print("\n1. Initializing database...")
    init_database("my_market_data.db")
    print("✓ Database initialized")
    
    # 2. Ingest TradingView CSV data
    print("\n2. Ingesting TradingView CSV data...")
    try:
        result = ingest_csv(
            file_path="TradingView-Feb9-CME_MINI_MNQ1!, 1_5cedc.csv",
            symbol="MNQ",
            timeframe="1m",
            source="tradingview",
            db_path="my_market_data.db"
        )
        print(f"✓ Ingested: {result['inserted']} bars")
        print(f"  Skipped: {result['skipped']} bars")
        print(f"  Conflicts: {result['conflicts']} bars")
        
        if result['conflicts'] > 0:
            print("\n  Conflict details:")
            for conflict in result['conflict_details'][:3]:  # Show first 3
                print(f"    - Timestamp: {conflict['timestamp']}")
                print(f"      Reason: {conflict['reason']}")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # 3. Add an observation about a trade day
    print("\n3. Adding trade day annotation...")
    try:
        # First, find a trade day that was ingested
        conn = sqlite3.connect("my_market_data.db")
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT session_date FROM trade_days WHERE symbol = 'MNQ' ORDER BY session_date LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        
        if row:
            session_date = row[0]
            annotation_id = save_day_annotation(
                symbol="MNQ",
                session_date=session_date,
                content="Strong momentum session with consistent trend",
                annotation_type="observation",
                tags=["momentum", "trend_day"],
                db_path="my_market_data.db"
            )
            print(f"✓ Saved annotation #{annotation_id} for {session_date}")
        else:
            print("  No trade days found to annotate")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # 4. Query bars for a specific day
    print("\n4. Querying bars...")
    try:
        conn = sqlite3.connect("my_market_data.db")
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT session_date FROM trade_days WHERE symbol = 'MNQ' ORDER BY session_date LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        
        if row:
            session_date = row[0]
            bars = get_bars(
                symbol="MNQ",
                session_date=session_date,
                db_path="my_market_data.db"
            )
            print(f"✓ Retrieved {len(bars)} bars for {session_date}")
            
            if bars:
                print(f"\n  First bar:")
                first_bar = bars[0]
                print(f"    Timestamp: {first_bar['timestamp']}")
                print(f"    Open: {first_bar['open']}")
                print(f"    High: {first_bar['high']}")
                print(f"    Low: {first_bar['low']}")
                print(f"    Close: {first_bar['close']}")
                print(f"    Volume: {first_bar['volume']}")
        else:
            print("  No trade days found")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # 5. Query annotations
    print("\n5. Querying annotations...")
    try:
        conn = sqlite3.connect("my_market_data.db")
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(session_date), MAX(session_date) FROM trade_days WHERE symbol = 'MNQ'")
        row = cursor.fetchone()
        conn.close()
        
        if row and row[0]:
            start_date, end_date = row[0], row[1]
            annotations = get_day_annotations(
                symbol="MNQ",
                start_date=start_date,
                end_date=end_date,
                tags=["momentum"],
                db_path="my_market_data.db"
            )
            print(f"✓ Found {len(annotations)} annotations with 'momentum' tag")
            
            for ann in annotations:
                print(f"\n  Annotation #{ann['id']}:")
                print(f"    Date: {ann['session_date']}")
                print(f"    Type: {ann['annotation_type']}")
                print(f"    Content: {ann['content']}")
                print(f"    Tags: {ann['tags']}")
        else:
            print("  No trade days found")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # 6. Example: Direct SQL query for learning
    print("\n6. Example SQL query (join annotations with trade days)...")
    try:
        conn = sqlite3.connect("my_market_data.db")
        cursor = conn.cursor()
        
        query = """
        SELECT 
            td.session_date,
            td.symbol,
            da.content,
            da.tags
        FROM trade_days td
        JOIN day_annotations da ON da.trade_day_id = td.id
        WHERE da.tags LIKE '%momentum%'
          AND da.status = 'active'
        ORDER BY td.session_date;
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print(f"✓ Found {len(rows)} results:")
        for row in rows:
            print(f"\n  {row[0]} - {row[1]}")
            print(f"    {row[2]}")
            print(f"    Tags: {row[3]}")
        
        conn.close()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
