"""
ClickHouse OPTIMIZE TABLE utility script.

This script helps with:
1. Checking background pool status
2. Running OPTIMIZE TABLE in synchronous mode
3. Monitoring merge operations
"""

from dotenv import load_dotenv
import clickhouse_connect
import os
import time

load_dotenv()

# --------------------
# Config (from .env)
# --------------------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))
DATABASE = os.getenv("DATABASE")
TABLE = os.getenv("TABLE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PWD")


def get_client():
    """Create and return ClickHouse client."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )


def check_background_pool_status(ch):
    """
    Check the status of ClickHouse background pool.
    
    Returns:
        dict: Dictionary with pool status information
    """
    query = """
    SELECT
        (SELECT value FROM system.metrics WHERE metric = 'BackgroundPoolTask') AS active_tasks,
        (SELECT value FROM system.metrics WHERE metric = 'BackgroundPoolSize') AS pool_size
    """
    
    result = ch.query(query)
    active_tasks = result.result_rows[0][0]
    pool_size = result.result_rows[0][1]
    free_threads = pool_size - active_tasks
    utilization_pct = (active_tasks / pool_size * 100) if pool_size > 0 else 0
    
    status = {
        'active_tasks': active_tasks,
        'pool_size': pool_size,
        'free_threads': free_threads,
        'utilization_pct': round(utilization_pct, 2),
        'is_full': free_threads <= 0
    }
    
    return status


def print_pool_status(status):
    """Print background pool status in a readable format."""
    print("\n" + "="*60)
    print("ClickHouse Background Pool Status")
    print("="*60)
    print(f"Active Tasks:      {status['active_tasks']}")
    print(f"Pool Size:         {status['pool_size']}")
    print(f"Free Threads:      {status['free_threads']}")
    print(f"Utilization:       {status['utilization_pct']}%")
    print(f"Pool Full:         {'YES ⚠️' if status['is_full'] else 'NO ✓'}")
    print("="*60 + "\n")


def check_merge_operations(ch, table_name=None):
    """
    Check current merge operations in ClickHouse.
    
    Args:
        ch: ClickHouse client
        table_name: Optional table name to filter by
    """
    if table_name:
        query = """
        SELECT 
            database,
            table,
            elapsed,
            progress,
            merge_type
        FROM system.merges
        WHERE table = %(table)s
        ORDER BY elapsed DESC
        """
        result = ch.query(query, parameters={'table': table_name})
    else:
        query = """
        SELECT 
            database,
            table,
            elapsed,
            progress,
            merge_type
        FROM system.merges
        ORDER BY elapsed DESC
        LIMIT 10
        """
        result = ch.query(query)
    
    if result.result_rows:
        print("\nCurrent Merge Operations:")
        print("-" * 60)
        for row in result.result_rows:
            db, tbl, elapsed, progress, merge_type = row
            print(f"  {db}.{tbl} | {merge_type} | Elapsed: {elapsed}s | Progress: {progress}")
        print("-" * 60 + "\n")
    else:
        print("\nNo active merge operations.\n")


def optimize_table_sync(ch, table_name, final=True, cleanup=False):
    """
    Run OPTIMIZE TABLE in synchronous mode.
    
    Args:
        ch: ClickHouse client
        table_name: Name of the table to optimize
        final: Whether to use FINAL (default: True)
        cleanup: Whether to use CLEANUP (default: False)
    
    Returns:
        bool: True if successful, False otherwise
    """
    # First, check pool status
    print("Checking background pool status before optimization...")
    status = check_background_pool_status(ch)
    print_pool_status(status)
    
    if status['is_full']:
        print("⚠️  Background pool is full. Running in synchronous mode...\n")
    else:
        print("✓ Background pool has capacity. Running in synchronous mode anyway for safety...\n")
    
    # Set alter_sync to 1 for synchronous execution
    print("Setting alter_sync = 1 (synchronous mode)...")
    ch.command("SET alter_sync = 1")
    
    # Build OPTIMIZE query
    optimize_query = f"OPTIMIZE TABLE {table_name}"
    if final:
        optimize_query += " FINAL"
    if cleanup:
        optimize_query += " CLEANUP"
    
    print(f"Executing: {optimize_query}")
    print("This may take a while depending on table size...\n")
    
    start_time = time.time()
    
    try:
        # Execute the OPTIMIZE command
        ch.command(optimize_query)
        elapsed = time.time() - start_time
        
        print(f"\n✓ OPTIMIZE completed successfully in {elapsed:.2f} seconds!")
        
        # Check pool status after
        print("\nChecking background pool status after optimization...")
        status_after = check_background_pool_status(ch)
        print_pool_status(status_after)
        
        return True
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n✗ OPTIMIZE failed after {elapsed:.2f} seconds")
        print(f"Error: {str(e)}")
        return False


def main():
    """Main function to demonstrate usage."""
    ch = get_client()
    
    print("\n" + "="*60)
    print("ClickHouse OPTIMIZE TABLE Utility")
    print("="*60)
    
    # Check current status
    print("\n1. Checking background pool status...")
    status = check_background_pool_status(ch)
    print_pool_status(status)
    
    # Check merge operations
    print("\n2. Checking active merge operations...")
    check_merge_operations(ch, TABLE)
    
    # Ask user if they want to proceed
    print("\n3. Ready to optimize table in synchronous mode.")
    print(f"   Table: {DATABASE}.{TABLE}")
    print("\n   Options:")
    print("   - OPTIMIZE TABLE ... FINAL (recommended for most cases)")
    print("   - OPTIMIZE TABLE ... FINAL CLEANUP (more aggressive, use with caution)")
    
    response = input("\n   Proceed with OPTIMIZE TABLE ... FINAL? (yes/no): ").strip().lower()
    
    if response in ['yes', 'y']:
        optimize_table_sync(ch, TABLE, final=True, cleanup=False)
    else:
        cleanup_response = input("   Proceed with OPTIMIZE TABLE ... FINAL CLEANUP? (yes/no): ").strip().lower()
        if cleanup_response in ['yes', 'y']:
            optimize_table_sync(ch, TABLE, final=True, cleanup=True)
        else:
            print("\n   Operation cancelled.")


if __name__ == "__main__":
    main()
