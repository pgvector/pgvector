# Search Defaults Index Option

## Overview

This feature adds per-index default search parameters (`default_probes` for IVFFlat and `default_ef_search` for HNSW) that automatically configure search behavior without requiring session-level `SET` commands. Index defaults take effect when no explicit session setting is active, while still respecting user overrides.

### Problem Solved

Before this feature, configuring search parameters like the number of IVFFlat probes or HNSW ef_search required:
- Setting session-level GUC variables before each query
- Wrapping queries in transaction blocks with `SET LOCAL` statements
- Using database-wide defaults that apply to all indexes

This created complexity when:
- Multiple tables/indexes require different optimal search values
- Partitioned tables have per-partition indexes needing varying settings
- Applications share database connections across different use cases

### Solution

Per-index defaults allow administrators to specify optimal search parameters at index creation time. Queries automatically use the appropriate settings based on which index is selected, eliminating the need for application code to manage parameter settings.

## Architecture and Design

### Precedence Rules

The effective search parameter follows a clear precedence hierarchy:

1. **Explicit `SET`** - Session-level `SET ivfflat.probes` / `SET hnsw.ef_search` takes highest priority
2. **Index default** - `default_probes` / `default_ef_search` option if set (value > 0)
3. **GUC default** - Falls back to `ivfflat.probes=1` / `hnsw.ef_search=40`

This design ensures:
- Users who explicitly SET a parameter get the value they expect
- Indexes without defaults behave identically to before (backward compatible)
- `RESET` returns to using the index default (if set), then GUC default

### Implementation Approach

The feature extends pgvector's existing patterns for index options:

1. **Index option structs** - Extended `IvfflatOptions` and `HnswOptions` with new integer fields
2. **Reloption registration** - New options registered during index access method initialization using `add_int_reloption()`
3. **Getter functions** - `IvfflatGetDefaultProbes()` and `HnswGetDefaultEfSearch()` retrieve configured values
4. **Effective value resolution** - `IvfflatGetEffectiveProbes()` and `HnswGetEffectiveEfSearch()` implement precedence logic
5. **GUC source detection** - Uses PostgreSQL's `find_option()` API to check if a GUC was explicitly set (`PGC_S_SESSION` source)

### Design Decisions

**Sentinel value of 0**: The value `0` serves as "unset" because valid values for probes and ef_search start at 1. This allows distinguishing between "explicitly set to a value" and "not configured" without requiring nullable options.

**Session-level detection via `PGC_S_SESSION`**: PostgreSQL's GUC infrastructure tracks the source of configuration values. When the source is `PGC_S_SESSION`, the user explicitly issued a `SET` command, which should take precedence over index defaults.

**Cost estimation integration**: Both IVFFlat and HNSW cost estimation functions use the effective search parameter value. This ensures the query planner makes decisions based on the actual search parameters that will be used at scan time.

## User Guide

### Creating Indexes with Search Defaults

#### IVFFlat

Specify `default_probes` when creating an IVFFlat index:

```sql
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) 
    WITH (lists = 100, default_probes = 10);
```

Queries using this index will automatically probe 10 lists without requiring a `SET` command.

#### HNSW

Specify `default_ef_search` when creating an HNSW index:

```sql
CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) 
    WITH (default_ef_search = 100);
```

Queries using this index will automatically use ef_search of 100.

### Overriding Index Defaults

Session-level `SET` commands always override index defaults:

```sql
-- Index has default_probes = 10, but this query uses 5 probes
SET ivfflat.probes = 5;
SELECT * FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 10;
```

Use `SET LOCAL` within a transaction for single-query overrides:

```sql
BEGIN;
SET LOCAL hnsw.ef_search = 200;
SELECT * FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 10;
COMMIT;
```

### Returning to Index Defaults

`RESET` clears the session-level setting, returning to the index default:

```sql
SET ivfflat.probes = 20;
-- ... queries use 20 probes ...
RESET ivfflat.probes;
-- ... queries now use index default_probes if set, otherwise GUC default
```

### Modifying Defaults After Creation

Change index defaults without rebuilding the index:

```sql
-- Add or change default
ALTER INDEX idx_items SET (default_probes = 15);

-- Remove default (fall back to GUC)
ALTER INDEX idx_items RESET (default_probes);
```

### Configuration

#### IVFFlat Parameters

| Parameter | Range | Default | Description |
|-----------|-------|---------|-------------|
| `default_probes` | 0 to 32768 | 0 (unset) | Default number of lists to probe during search |

#### HNSW Parameters

| Parameter | Range | Default | Description |
|-----------|-------|---------|-------------|
| `default_ef_search` | 0 to 1000 | 0 (unset) | Default size of dynamic candidate list for search |

## Technical Reference

### Modified Components

| File | Component | Changes |
|------|-----------|---------|
| `src/ivfflat.h` | `IvfflatOptions` struct | Added `defaultProbes` field |
| `src/ivfflat.c` | `IvfflatInit()` | Registered `default_probes` reloption |
| `src/ivfflat.c` | `ivfflatoptions()` | Added parsing entry |
| `src/ivfflat.c` | `ivfflatcostestimate()` | Uses effective probes for cost calculation |
| `src/ivfutils.c` | New functions | `IvfflatGetDefaultProbes()`, `IvfflatGetEffectiveProbes()` |
| `src/ivfscan.c` | `ivfflatbeginscan()` | Uses effective probes |
| `src/hnsw.h` | `HnswOptions` struct | Added `defaultEfSearch` field |
| `src/hnsw.c` | `HnswInit()` | Registered `default_ef_search` reloption |
| `src/hnsw.c` | `hnswoptions()` | Added parsing entry |
| `src/hnsw.c` | `hnswcostestimate()` | Uses effective ef_search for cost calculation |
| `src/hnswutils.c` | New functions | `HnswGetDefaultEfSearch()`, `HnswGetEffectiveEfSearch()` |
| `src/hnswscan.c` | `GetScanItems()`, `ResumeScanItems()` | Uses effective ef_search |

### Key Functions

#### `IvfflatGetEffectiveProbes(Relation index)`

Returns the effective probes value for an IVFFlat index:
1. Checks if `ivfflat.probes` was explicitly SET in session
2. Falls back to index `default_probes` if set (> 0)
3. Falls back to GUC default value

#### `HnswGetEffectiveEfSearch(Relation index)`

Returns the effective ef_search value for an HNSW index:
1. Checks if `hnsw.ef_search` was explicitly SET in session
2. Falls back to index `default_ef_search` if set (> 0)
3. Falls back to GUC default value

### Error Handling

Invalid option values are rejected at index creation/alter time:

```sql
-- Fails: negative value not allowed
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) 
    WITH (default_probes = -1);
-- ERROR: value -1 out of bounds for option "default_probes"
```

## Usage Examples

### Example 1: Different Settings per Table

E-commerce platform with different accuracy requirements:

```sql
-- Product search: speed-optimized with fewer probes
CREATE TABLE products (id serial, embedding vector(512));
CREATE INDEX ON products USING ivfflat (embedding vector_l2_ops) 
    WITH (lists = 100, default_probes = 3);

-- Support tickets: accuracy-optimized with more probes
CREATE TABLE tickets (id serial, embedding vector(512));
CREATE INDEX ON tickets USING ivfflat (embedding vector_l2_ops) 
    WITH (lists = 100, default_probes = 20);

-- Queries automatically use appropriate settings
SELECT * FROM products ORDER BY embedding <-> $1 LIMIT 10;  -- 3 probes
SELECT * FROM tickets ORDER BY embedding <-> $1 LIMIT 10;  -- 20 probes
```

### Example 2: Partitioned Tables

Different partitions can have tailored search parameters:

```sql
CREATE TABLE vectors (
    id serial,
    category text,
    embedding vector(256)
) PARTITION BY LIST (category);

CREATE TABLE vectors_hot PARTITION OF vectors FOR VALUES IN ('hot');
CREATE TABLE vectors_archive PARTITION OF vectors FOR VALUES IN ('archive');

-- Hot data: optimized for speed
CREATE INDEX ON vectors_hot USING hnsw (embedding vector_l2_ops) 
    WITH (default_ef_search = 40);

-- Archive: optimized for recall
CREATE INDEX ON vectors_archive USING hnsw (embedding vector_l2_ops) 
    WITH (default_ef_search = 200);
```

### Example 3: Temporary Override for Specific Query

When you need higher accuracy for a specific search:

```sql
-- Index has default_probes = 5
BEGIN;
SET LOCAL ivfflat.probes = 50;
SELECT * FROM items 
WHERE category = 'important' 
ORDER BY embedding <-> $1 
LIMIT 100;
COMMIT;
-- Subsequent queries return to using index default
```

## Edge Cases and Limitations

### Value of 0 Means "Unset"

Setting `default_probes = 0` or `default_ef_search = 0` is equivalent to not setting the option:

```sql
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) 
    WITH (lists = 100, default_probes = 0);
-- Queries use ivfflat.probes GUC value (default: 1)
```

### Clamping to Valid Ranges

The `default_probes` and `default_ef_search` values are validated against the maximum allowed values (`IVFFLAT_MAX_LISTS` and `HNSW_MAX_EF_SEARCH` respectively). The existing clamping behavior for search parameters applies at query time.

### Session Isolation

Each session maintains its own GUC state. Explicit `SET` in one session does not affect other sessions:

```sql
-- Session A
SET ivfflat.probes = 20;
-- Session A queries use 20 probes

-- Session B (concurrent)
-- Session B queries use index default (no SET active)
```

### SET LOCAL Behavior

Both `SET` and `SET LOCAL` are treated as explicit user overrides:

```sql
BEGIN;
SET LOCAL hnsw.ef_search = 200;
-- Uses 200, overriding any index default
COMMIT;
-- Outside transaction: uses index default
```

## Testing Guide

### How to Test This Feature

1. **Create an index with a default:**
   ```sql
   CREATE TABLE test_vectors (id serial, embedding vector(3));
   INSERT INTO test_vectors (embedding) 
       SELECT ARRAY[random(), random(), random()]::vector FROM generate_series(1, 1000);
   CREATE INDEX ON test_vectors USING ivfflat (embedding vector_l2_ops) 
       WITH (lists = 10, default_probes = 5);
   ```

2. **Verify index default is used:**
   ```sql
   SET enable_seqscan = off;
   EXPLAIN (ANALYZE) SELECT * FROM test_vectors ORDER BY embedding <-> '[0.5, 0.5, 0.5]' LIMIT 5;
   -- Note: The actual probes value is not visible in EXPLAIN output, but query behavior reflects it
   ```

3. **Test explicit SET override:**
   ```sql
   SET ivfflat.probes = 2;
   SELECT * FROM test_vectors ORDER BY embedding <-> '[0.5, 0.5, 0.5]' LIMIT 5;
   -- Query uses 2 probes (explicit SET takes precedence)
   ```

4. **Test RESET returns to index default:**
   ```sql
   RESET ivfflat.probes;
   SELECT * FROM test_vectors ORDER BY embedding <-> '[0.5, 0.5, 0.5]' LIMIT 5;
   -- Query uses 5 probes (index default)
   ```

5. **Test ALTER INDEX:**
   ```sql
   ALTER INDEX test_vectors_embedding_idx SET (default_probes = 8);
   SELECT * FROM test_vectors ORDER BY embedding <-> '[0.5, 0.5, 0.5]' LIMIT 5;
   -- Query now uses 8 probes
   ```

6. **Test removing the default:**
   ```sql
   ALTER INDEX test_vectors_embedding_idx RESET (default_probes);
   SELECT * FROM test_vectors ORDER BY embedding <-> '[0.5, 0.5, 0.5]' LIMIT 5;
   -- Query uses GUC default (1 probe)
   ```

## Migration and Compatibility

### Backward Compatibility

- **Existing indexes**: Indexes created without the new options behave identically to before (use GUC values)
- **Existing queries**: No query changes required; the feature is opt-in via index options
- **No data migration**: Index metadata automatically supports the new options after pgvector upgrade

### Upgrade Path

1. Upgrade pgvector to version with this feature
2. Optionally modify existing indexes to add defaults:
   ```sql
   ALTER INDEX my_index SET (default_probes = 10);
   ```
3. No rebuild required

## References

- Original Issue: https://github.com/pgvector/pgvector/issues/235
- Specification: `.paw/work/235-search-defaults-index-option/Spec.md`
- Implementation Plan: `.paw/work/235-search-defaults-index-option/ImplementationPlan.md`
- PostgreSQL GUC Documentation: https://www.postgresql.org/docs/current/config-setting.html
