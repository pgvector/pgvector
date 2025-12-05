# Search Defaults Index Option Implementation Plan

## Overview

This implementation adds per-index default search parameters (`default_probes` for IVFFlat and `default_ef_search` for HNSW) that override session-level GUC defaults while respecting explicit user SET commands. The feature addresses use cases where users manage multiple vector indexes with different optimal search parameters and want to avoid per-query SET statements.

## Current State Analysis

pgvector currently supports session-level GUCs (`ivfflat.probes`, `hnsw.ef_search`) that apply globally to all queries. Users must issue `SET` commands before each query to customize search parameters, creating complexity when:
- Multiple tables/indexes require different default values
- Partitioned tables have per-partition indexes with varying optimal settings
- Applications share database connections across different use cases

### Key Discoveries:
- Index options are defined in `IvfflatOptions` (`src/ivfflat.h:117-121`) and `HnswOptions` (`src/hnsw.h:181-186`) structs
- Options are registered via `add_int_reloption()` in `IvfflatInit()` (`src/ivfflat.c:35-37`) and `HnswInit()` (`src/hnsw.c:81-84`)
- Existing getter pattern in `IvfflatGetLists()` (`src/ivfutils.c:44-53`) and `HnswGetM()` (`src/hnswutils.c:110-118`)
- GUC source tracking via `find_option()` API returns `config_generic*` with `source` field of type `GucSource`
- Scan functions receive `Relation index` with access to `rd_options` for reading index defaults
- Cost estimation functions already open the index relation to read metadata

## Desired End State

After implementation:
1. Users can create indexes with `default_probes`/`default_ef_search` options
2. Queries automatically use index defaults when no explicit session SET is active
3. Explicit `SET ivfflat.probes`/`SET hnsw.ef_search` overrides index defaults
4. `ALTER INDEX ... SET/RESET` modifies defaults without index rebuild
5. Cost estimation reflects the effective search parameter value

### Verification:
- SQL tests demonstrate index defaults are used when GUC is at default source
- SQL tests demonstrate explicit SET takes precedence over index defaults
- Cost estimation produces different results based on index defaults

## What We're NOT Doing

- "Smart" automatic defaults based on data characteristics (separate future work per Issue #235)
- Query hint syntax (requires PostgreSQL parser changes)
- Per-query parameter overrides without SET command
- Default values for other search parameters (iterative_scan, max_probes, etc.)
- SQL migration scripts (index options automatically available after C registration)

## Implementation Approach

The implementation follows pgvector's established patterns for index options:

1. **Extend option structs** with new integer fields for search defaults
2. **Register reloptions** during init using `add_int_reloption()` with 0 as "unset" sentinel
3. **Create getter functions** following `IvfflatGetLists()`/`HnswGetM()` pattern
4. **Add GUC source detection** utility to check if a GUC was explicitly SET in session
5. **Create effective value resolution** functions that implement the precedence: explicit SET > index default > GUC default
6. **Update scan functions** to use effective values instead of direct GUC access
7. **Update cost estimation** to use effective values for accurate planning

## Phase Summary

1. **Phase 1: Index Option Infrastructure** - Extend structs, register reloptions, create getter functions
2. **Phase 2: GUC Resolution Logic** - Add GUC source detection and effective value resolution functions  
3. **Phase 3: Scan Integration** - Modify scan functions to use resolved effective values
4. **Phase 4: Cost Estimation** - Update cost functions to use resolved values
5. **Phase 5: Tests** - Add comprehensive test coverage for all acceptance scenarios

---

## Phase 1: Index Option Infrastructure

### Overview
Extend the index option infrastructure to support the new `default_probes` and `default_ef_search` options. This phase establishes the foundation without changing runtime behavior.

### Changes Required:

#### 1. IVFFlat Option Struct Extension
**File**: `src/ivfflat.h`
**Changes**:
- Add `int defaultProbes` field to `IvfflatOptions` struct after `lists`

**Tests**:
- Compilation succeeds
- Existing IVFFlat tests pass (no behavioral change yet)

#### 2. HNSW Option Struct Extension  
**File**: `src/hnsw.h`
**Changes**:
- Add `int defaultEfSearch` field to `HnswOptions` struct after `efConstruction`

**Tests**:
- Compilation succeeds
- Existing HNSW tests pass (no behavioral change yet)

#### 3. IVFFlat Option Registration
**File**: `src/ivfflat.c`
**Changes**:
- In `IvfflatInit()`, register `default_probes` option using `add_int_reloption()`:
  - Name: `"default_probes"`
  - Description: `"Default number of probes for searches"`
  - Default: `0` (sentinel for "unset")
  - Min: `0`, Max: `IVFFLAT_MAX_LISTS`
  - Lock: `AccessExclusiveLock`
- In `ivfflatoptions()`, add to `relopt_parse_elt tab[]`:
  - `{"default_probes", RELOPT_TYPE_INT, offsetof(IvfflatOptions, defaultProbes)}`

**Tests**:
- `CREATE INDEX ... WITH (default_probes = 5)` succeeds
- `CREATE INDEX ... WITH (default_probes = -1)` fails validation
- `CREATE INDEX ... WITH (default_probes = 0)` succeeds (unset sentinel)

#### 4. HNSW Option Registration
**File**: `src/hnsw.c`
**Changes**:
- In `HnswInit()`, register `default_ef_search` option using `add_int_reloption()`:
  - Name: `"default_ef_search"`
  - Description: `"Default ef_search value for searches"`
  - Default: `0` (sentinel for "unset")
  - Min: `0`, Max: `HNSW_MAX_EF_SEARCH`
  - Lock: `AccessExclusiveLock`
- In `hnswoptions()`, add to `relopt_parse_elt tab[]`:
  - `{"default_ef_search", RELOPT_TYPE_INT, offsetof(HnswOptions, defaultEfSearch)}`

**Tests**:
- `CREATE INDEX ... WITH (default_ef_search = 100)` succeeds
- `CREATE INDEX ... WITH (default_ef_search = -1)` fails validation
- `CREATE INDEX ... WITH (default_ef_search = 0)` succeeds (unset sentinel)

#### 5. IVFFlat Getter Function
**File**: `src/ivfutils.c`
**Changes**:
- Add `IvfflatGetDefaultProbes(Relation index)` function following `IvfflatGetLists()` pattern
- Returns the `defaultProbes` value from `rd_options`, or `0` if NULL/unset

**File**: `src/ivfflat.h`
**Changes**:
- Add extern declaration for `IvfflatGetDefaultProbes()`

**Tests**:
- Unit test: function returns 0 when no option set
- Unit test: function returns configured value when option set

#### 6. HNSW Getter Function
**File**: `src/hnswutils.c`
**Changes**:
- Add `HnswGetDefaultEfSearch(Relation index)` function following `HnswGetM()` pattern
- Returns the `defaultEfSearch` value from `rd_options`, or `0` if NULL/unset

**File**: `src/hnsw.h`
**Changes**:
- Add extern declaration for `HnswGetDefaultEfSearch()`

**Tests**:
- Unit test: function returns 0 when no option set
- Unit test: function returns configured value when option set

### Success Criteria:

#### Automated Verification:
- [x] Code compiles without warnings: `make clean && make`
- [x] Existing regression tests pass: `make installcheck`
- [ ] New index option is accepted: SQL test with `WITH (default_probes = N)` (deferred to Phase 5)
- [ ] Invalid option values rejected: SQL test with out-of-range values (deferred to Phase 5)

#### Manual Verification:
- [ ] `\d+ index_name` shows the new option in index properties (if PostgreSQL displays reloptions)

### Phase 1 Status: COMPLETE

**Completed**: 2025-12-05

**Summary**: Added index option infrastructure for `default_probes` (IVFFlat) and `default_ef_search` (HNSW). Extended option structs with new integer fields, registered reloptions with `add_int_reloption()` using 0 as the "unset" sentinel, added parsing table entries, and implemented getter functions following existing patterns. All 14 existing regression tests pass.

**Commit**: 7f301a1 - "Add index option infrastructure for default_probes and default_ef_search"

**Notes for reviewers**: The new options are now accepted by PostgreSQL but don't affect runtime behavior yet. Phase 2 will add the GUC source detection logic to implement the precedence rules.

---

## Phase 2: GUC Resolution Logic

### Overview
Add utility functions to detect whether a GUC was explicitly set in the current session and to resolve the effective search parameter value based on the precedence rules.

### Changes Required:

#### 1. GUC Source Detection Utility
**File**: `src/ivfflat.c` (or new shared utility file if preferred)
**Changes**:
- Add static helper function `IsGucExplicitlySet(const char *guc_name)` that:
  - Calls `find_option(guc_name, false, true, ERROR)` to get `config_generic*`
  - Returns `true` if `record->source == PGC_S_SESSION`
  - Returns `false` otherwise (includes default, file, database, user settings)
- Include necessary header: `#include "utils/guc_tables.h"`

**Tests**:
- Integration test: function returns false before any SET
- Integration test: function returns true after `SET ivfflat.probes = N`
- Integration test: function returns false after `RESET ivfflat.probes`

#### 2. IVFFlat Effective Probes Resolution
**File**: `src/ivfflat.c` or `src/ivfutils.c`
**Changes**:
- Add function `IvfflatGetEffectiveProbes(Relation index)` that implements:
  1. Check if `IsGucExplicitlySet("ivfflat.probes")` → return `ivfflat_probes`
  2. Get `defaultProbes = IvfflatGetDefaultProbes(index)`
  3. If `defaultProbes > 0` → return `defaultProbes`
  4. Return `ivfflat_probes` (GUC default)
- Add extern declaration in `src/ivfflat.h`

**Tests**:
- Test: returns GUC default when no index default and no explicit SET
- Test: returns index default when set and no explicit SET
- Test: returns explicit SET value regardless of index default

#### 3. HNSW Effective ef_search Resolution
**File**: `src/hnsw.c` or `src/hnswutils.c`
**Changes**:
- Add function `HnswGetEffectiveEfSearch(Relation index)` that implements:
  1. Check if `IsGucExplicitlySet("hnsw.ef_search")` → return `hnsw_ef_search`
  2. Get `defaultEfSearch = HnswGetDefaultEfSearch(index)`
  3. If `defaultEfSearch > 0` → return `defaultEfSearch`
  4. Return `hnsw_ef_search` (GUC default)
- Add extern declaration in `src/hnsw.h`

**Tests**:
- Test: returns GUC default when no index default and no explicit SET
- Test: returns index default when set and no explicit SET
- Test: returns explicit SET value regardless of index default

### Success Criteria:

#### Automated Verification:
- [ ] Code compiles without warnings: `make clean && make`
- [ ] Existing regression tests pass: `make installcheck`
- [ ] GUC source detection correctly identifies explicit SET

#### Manual Verification:
- [ ] Verify behavior with `SET LOCAL` within transaction (should also override)

---

## Phase 3: Scan Integration

### Overview
Modify the scan initialization functions to use the effective search parameter values instead of directly reading the GUC variables.

### Changes Required:

#### 1. IVFFlat Scan Integration
**File**: `src/ivfscan.c`
**Changes**:
- In `ivfflatbeginscan()` at line 248, replace:
  ```c
  int probes = ivfflat_probes;
  ```
  with:
  ```c
  int probes = IvfflatGetEffectiveProbes(index);
  ```

**Tests**:
- Test in `test/sql/ivfflat_vector.sql`:
  - Create index with `default_probes = 5`, query without SET, verify probes used
  - SET `ivfflat.probes = 20`, query, verify 20 probes used
  - RESET `ivfflat.probes`, query, verify index default (5) used

#### 2. HNSW Scan Integration
**File**: `src/hnswscan.c`
**Changes**:
- In `GetScanItems()` at line 43, replace direct `hnsw_ef_search` usage with:
  ```c
  int efSearch = HnswGetEffectiveEfSearch(index);
  ```
  Pass `efSearch` to `HnswSearchLayer()` instead of `hnsw_ef_search`
- Similarly update `ResumeScanItems()` at line 55 where `hnsw_ef_search` is used

**Tests**:
- Test in `test/sql/hnsw_vector.sql`:
  - Create index with `default_ef_search = 100`, query without SET, verify ef_search used
  - SET `hnsw.ef_search = 200`, query, verify 200 used
  - RESET `hnsw.ef_search`, query, verify index default (100) used

### Success Criteria:

#### Automated Verification:
- [ ] Code compiles without warnings: `make clean && make`
- [ ] Existing regression tests pass: `make installcheck`
- [ ] New SQL tests pass verifying precedence logic

#### Manual Verification:
- [ ] Query uses index default when no explicit SET
- [ ] Explicit SET overrides index default
- [ ] RESET returns to using index default

---

## Phase 4: Cost Estimation

### Overview
Update the cost estimation functions to use the effective search parameter values for accurate query planning.

### Changes Required:

#### 1. IVFFlat Cost Estimation
**File**: `src/ivfflat.c`
**Changes**:
- In `ivfflatcostestimate()`, after opening the index (around line 112-114):
  - Get effective probes: `int effectiveProbes = IvfflatGetEffectiveProbes(index);`
  - Replace line 117 `ratio = ((double) ivfflat_probes) / lists;` with:
    ```c
    ratio = ((double) effectiveProbes) / lists;
    ```

**Tests**:
- Test: EXPLAIN shows different costs for indexes with different default_probes values

#### 2. HNSW Cost Estimation
**File**: `src/hnsw.c`
**Changes**:
- In `hnswcostestimate()`, after opening the index (around line 160):
  - Get effective ef_search: `int effectiveEfSearch = HnswGetEffectiveEfSearch(index);`
  - Replace line 194 usage of `hnsw_ef_search` with `effectiveEfSearch`:
    ```c
    int layer0TuplesMax = HnswGetLayerM(m, 0) * effectiveEfSearch;
    ```
  - Replace line 195 usage of `hnsw_ef_search` with `effectiveEfSearch`:
    ```c
    double layer0Selectivity = scalingFactor * log(path->indexinfo->tuples) / (log(m) * (1 + log(effectiveEfSearch)));
    ```

**Tests**:
- Test: EXPLAIN shows different costs for indexes with different default_ef_search values

### Success Criteria:

#### Automated Verification:
- [ ] Code compiles without warnings: `make clean && make`
- [ ] Existing regression tests pass: `make installcheck`
- [ ] Cost estimates reflect index default values in EXPLAIN output

#### Manual Verification:
- [ ] Query planner chooses expected index when multiple indexes have different defaults
- [ ] Cost estimates change appropriately with SET commands

---

## Phase 5: Tests

### Overview
Add comprehensive test coverage for all acceptance scenarios defined in the specification.

### Changes Required:

#### 1. IVFFlat Test Cases
**File**: `test/sql/ivfflat_vector.sql`
**Changes**:
- Add test section for `default_probes` option:

```sql
-- default_probes option tests

-- Test: Create index with default_probes
CREATE TABLE t_dp (val vector(3));
INSERT INTO t_dp (val) SELECT ARRAY[random(), random(), random()]::vector FROM generate_series(1, 100);
CREATE INDEX idx_dp ON t_dp USING ivfflat (val vector_l2_ops) WITH (lists = 10, default_probes = 5);

-- Test: Query uses index default (visual verification via EXPLAIN)
EXPLAIN (COSTS OFF) SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;
SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: Explicit SET overrides index default
SET ivfflat.probes = 3;
EXPLAIN (COSTS OFF) SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;
SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: RESET returns to index default
RESET ivfflat.probes;
SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: ALTER INDEX changes default
ALTER INDEX idx_dp SET (default_probes = 8);
SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: ALTER INDEX RESET removes default
ALTER INDEX idx_dp RESET (default_probes);
SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: default_probes = 0 acts as unset
CREATE INDEX idx_dp_zero ON t_dp USING ivfflat (val vector_l2_ops) WITH (lists = 10, default_probes = 0);

-- Test: Invalid values rejected
CREATE INDEX ON t_dp USING ivfflat (val vector_l2_ops) WITH (default_probes = -1);

DROP TABLE t_dp;
```

**File**: `test/expected/ivfflat_vector.out`
**Changes**:
- Add expected output corresponding to new test cases

**Tests**:
- All new SQL tests pass

#### 2. HNSW Test Cases
**File**: `test/sql/hnsw_vector.sql`
**Changes**:
- Add test section for `default_ef_search` option:

```sql
-- default_ef_search option tests

-- Test: Create index with default_ef_search
CREATE TABLE t_des (val vector(3));
INSERT INTO t_des (val) SELECT ARRAY[random(), random(), random()]::vector FROM generate_series(1, 100);
CREATE INDEX idx_des ON t_des USING hnsw (val vector_l2_ops) WITH (default_ef_search = 100);

-- Test: Query uses index default (visual verification via EXPLAIN)
EXPLAIN (COSTS OFF) SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;
SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: Explicit SET overrides index default
SET hnsw.ef_search = 50;
EXPLAIN (COSTS OFF) SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;
SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: RESET returns to index default
RESET hnsw.ef_search;
SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: ALTER INDEX changes default
ALTER INDEX idx_des SET (default_ef_search = 200);
SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: ALTER INDEX RESET removes default
ALTER INDEX idx_des RESET (default_ef_search);
SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

-- Test: default_ef_search = 0 acts as unset
CREATE INDEX idx_des_zero ON t_des USING hnsw (val vector_l2_ops) WITH (default_ef_search = 0);

-- Test: Invalid values rejected
CREATE INDEX ON t_des USING hnsw (val vector_l2_ops) WITH (default_ef_search = -1);

DROP TABLE t_des;
```

**File**: `test/expected/hnsw_vector.out`
**Changes**:
- Add expected output corresponding to new test cases

**Tests**:
- All new SQL tests pass

#### 3. Multiple Index Test
**File**: `test/sql/ivfflat_vector.sql` (or separate file)
**Changes**:
- Add test for multiple indexes with different defaults:

```sql
-- Multiple indexes with different defaults
CREATE TABLE t_multi (id int, val vector(3));
INSERT INTO t_multi SELECT i, ARRAY[random(), random(), random()]::vector FROM generate_series(1, 100) i;

CREATE INDEX idx_multi_low ON t_multi USING ivfflat (val vector_l2_ops) WITH (lists = 10, default_probes = 2);
CREATE INDEX idx_multi_high ON t_multi USING ivfflat (val vector_l2_ops) WITH (lists = 10, default_probes = 8);

-- Force use of specific index and verify different behavior
SET enable_seqscan = off;
SELECT * FROM t_multi ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5;

DROP TABLE t_multi;
```

**Tests**:
- Verify different indexes use their respective defaults

### Success Criteria:

#### Automated Verification:
- [ ] All new tests pass: `make installcheck`
- [ ] No regressions in existing tests
- [ ] Test coverage for all acceptance scenarios from Spec.md

#### Manual Verification:
- [ ] Review expected output files for correctness
- [ ] Verify edge cases (EC-001 through EC-006) are covered

---

## Cross-Phase Testing Strategy

### Integration Tests:
- End-to-end: Create index with default, query, SET override, query, RESET, query
- Multiple sessions: Verify defaults are session-independent
- Partitioned tables: Each partition's index uses its own default

### Manual Testing Steps:
1. Build and install pgvector with changes
2. Create database with extension
3. Create test table with vectors
4. Create index with `default_probes = 5`
5. Run query without SET - verify uses 5 probes
6. Run `SET ivfflat.probes = 10`
7. Run query - verify uses 10 probes
8. Run `RESET ivfflat.probes`
9. Run query - verify uses 5 probes again (index default)
10. Repeat for HNSW with `default_ef_search`

## Performance Considerations

The implementation adds minimal overhead:
- `find_option()` call during scan initiation: O(hash lookup), negligible compared to I/O
- Index option read via `rd_options`: Already in memory, single pointer dereference
- No additional memory allocations in hot path

If performance testing reveals measurable overhead, consider caching the GUC source check result in the scan opaque structure (checked once at beginscan, reused throughout scan).

## Migration Notes

- No SQL migration required - index options are automatically available after C code registration
- Existing indexes without new options behave identically to current behavior
- No data migration needed - options stored in index metadata automatically

## References

- Original Issue: https://github.com/pgvector/pgvector/issues/235
- Spec: `.paw/work/235-search-defaults-index-option/Spec.md`
- Research: `.paw/work/235-search-defaults-index-option/SpecResearch.md`, `.paw/work/235-search-defaults-index-option/CodeResearch.md`
- Similar implementation patterns: `IvfflatGetLists()` at `src/ivfutils.c:44`, `HnswGetM()` at `src/hnswutils.c:110`
