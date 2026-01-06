# ClickHouse OPTIMIZE TABLE - Quick Reference

## Problem
When running `OPTIMIZE TABLE table FINAL CLEANUP`, you get:
```
Cannot run optimize, background pool is already full, try to execute in synchronous mode (alter_sync >= 1).
```

## Solution: Run in Synchronous Mode

### Method 1: Using SQL (ClickHouse CLI or any SQL client)

```sql
-- Set synchronous mode
SET alter_sync = 1;

-- Then run your OPTIMIZE command
OPTIMIZE TABLE your_table_name FINAL CLEANUP;
```

### Method 2: Using Python (clickhouse-connect)

```python
import clickhouse_connect

ch = clickhouse_connect.get_client(
    host="your_host",
    port=8123,
    database="your_database",
    user="your_user",
    password="your_password"
)

# Set synchronous mode
ch.command("SET alter_sync = 1")

# Run OPTIMIZE
ch.command("OPTIMIZE TABLE your_table_name FINAL CLEANUP")
```

## Check Background Pool Status

### SQL Query

```sql
SELECT
    (SELECT value FROM system.metrics WHERE metric = 'BackgroundPoolTask') AS active_tasks,
    (SELECT value FROM system.metrics WHERE metric = 'BackgroundPoolSize') AS pool_size,
    pool_size - active_tasks AS free_threads,
    ROUND((active_tasks / pool_size * 100), 2) AS utilization_pct
FROM system.metrics
LIMIT 1;
```

### Python Function

```python
def check_background_pool(ch):
    query = """
    SELECT
        (SELECT value FROM system.metrics WHERE metric = 'BackgroundPoolTask') AS active_tasks,
        (SELECT value FROM system.metrics WHERE metric = 'BackgroundPoolSize') AS pool_size
    """
    result = ch.query(query)
    active_tasks = result.result_rows[0][0]
    pool_size = result.result_rows[0][1]
    free_threads = pool_size - active_tasks
    
    print(f"Active Tasks: {active_tasks}")
    print(f"Pool Size: {pool_size}")
    print(f"Free Threads: {free_threads}")
    print(f"Pool Full: {free_threads <= 0}")
    
    return {
        'active_tasks': active_tasks,
        'pool_size': pool_size,
        'free_threads': free_threads,
        'is_full': free_threads <= 0
    }
```

## Check Active Merge Operations

```sql
SELECT 
    database,
    table,
    elapsed,
    progress,
    merge_type
FROM system.merges
ORDER BY elapsed DESC;
```

## Understanding alter_sync

- `alter_sync = 0` (default): Asynchronous mode - operation returns immediately, runs in background
- `alter_sync = 1`: Synchronous mode - waits for operation to complete on current replica
- `alter_sync = 2`: Synchronous mode - waits for operation to complete on all replicas (for replicated tables)

## Important Notes

1. **OPTIMIZE TABLE ... FINAL CLEANUP** is resource-intensive and should be used:
   - During maintenance windows
   - When you have specific cleanup needs
   - Not as a routine operation

2. **Synchronous mode** will block until the operation completes, which can take a long time for large tables.

3. **Background pool** is shared across all background operations (merges, mutations, etc.). If it's full, you may need to:
   - Wait for current operations to complete
   - Run OPTIMIZE in synchronous mode (as shown above)
   - Consider increasing `background_pool_size` in server config (requires restart)

4. **ClickHouse Cloud**: You may have limited control over `background_pool_size` depending on your plan. Using synchronous mode is often the best solution.

## Using the Utility Script

Run the provided `optimize_table.py` script:

```bash
python optimize_table.py
```

This script will:
- Check background pool status
- Show active merge operations
- Run OPTIMIZE in synchronous mode with proper error handling
