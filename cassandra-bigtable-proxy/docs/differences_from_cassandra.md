# Differences From Cassandra

This document outlines the key differences between Apache Cassandra and the Cassandra to Bigtable Proxy. Understanding these differences is crucial for successfully migrating applications from Cassandra to Bigtable.

## USING TIMESTAMP Behavior

### Cassandra
In Cassandra, USING TIMESTAMP can be applied to INSERT, UPDATE, and DELETE operations. The timestamp is applied at the row level, ensuring consistent time-based operations across all cells in a row.

### Proxy (Bigtable)
The proxy does not support USING TIMESTAMP for DELETE operations. This limitation exists because:

- Bigtable manages timestamps at the cell level, while Cassandra manages them at the row level
- This fundamental architectural difference can lead to data inconsistencies when using USING TIMESTAMP with DELETE operations
- Specifically, after a DELETE operation with USING TIMESTAMP, a subsequent SELECT might return older data or revisions that should have been deleted, potentially leading to incorrect results

For this reason, the proxy restricts USING TIMESTAMP to INSERT and UPDATE operations only, where the cell-level timestamp management in Bigtable can be safely mapped to Cassandra's row-level behavior.

## ORDER BY Clause Behavior

### Cassandra
In Cassandra, the ORDER BY clause has specific requirements:

1. It can only be applied to clustering columns (columns that are part of the primary key but not the partition key)
2. The partition key must be fully restricted with an equality condition (EQ) or an IN condition
3. You cannot use ORDER BY when querying across multiple partitions

For example, in a table with primary key `((partition_key), clustering_col1, clustering_col2)`:
- `SELECT * FROM table WHERE partition_key = 'value' ORDER BY clustering_col1 ASC` - This is valid
- `SELECT * FROM table WHERE partition_key IN ('value1', 'value2') ORDER BY clustering_col1 ASC` - This is valid
- `SELECT * FROM table WHERE partition_key > 'value' ORDER BY clustering_col1 ASC` - This is not valid
- `SELECT * FROM table ORDER BY clustering_col1 ASC` - This is not valid (no partition key restriction)

The ORDER BY clause must reference clustering columns and can only change their natural order (ASC or DESC).

### Proxy (Bigtable)
The proxy allows ORDER BY to be applied to any column, not just clustering columns. Additionally, the proxy does not enforce the same restriction on partition key conditions. This provides more flexibility in querying data but differs from Cassandra's behavior.

## TTL (Time-To-Live) Behavior

### Cassandra
In Cassandra, TTL is managed at the cell level. Each cell can have its own TTL value, allowing for fine-grained control over data expiration.

### Proxy (Bigtable)
The proxy does not support TTL because Bigtable manages TTL at the column family level, not at the cell level. This fundamental architectural difference makes it impossible to implement cell-level TTL in the proxy.

## Other Differences

- **Secondary Indexes**: Cassandra supports secondary indexes, but the proxy does not
- **Materialized Views**: Cassandra supports materialized views, but the proxy does not
- **Batch Operations**: The proxy has limitations on batch operations compared to Cassandra
- **Consistency Levels**: The proxy does not support all Cassandra consistency levels
- **Counter Columns**: The proxy handles counter columns differently from Cassandra

For a comprehensive list of limitations, please refer to the [Limitations](./limitations.md) document.
