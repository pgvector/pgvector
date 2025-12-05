---
date: 2025-12-04T23:44:31-05:00
git_commit: 1f7000c79718445ebe6af9b0431c16b9a1f84f94
branch: feature/235-search-defaults-index-option
repository: pgvector
topic: "Search Defaults Index Option Implementation"
tags: [research, codebase, ivfflat, hnsw, index-options, guc, reloptions]
status: complete
last_updated: 2025-12-04
---

# Code Research: Search Defaults Index Option

**Date**: 2025-12-04 23:44:31 EST  
**Git Commit**: 1f7000c79718445ebe6af9b0431c16b9a1f84f94  
**Branch**: feature/235-search-defaults-index-option  
**Repository**: pgvector

## Research Question

Document the implementation details required to add per-index default search parameters (`default_probes` for IVFFlat and `default_ef_search` for HNSW) that override session-level GUC defaults while respecting explicit user SET commands.

## Summary

This research documents the existing pgvector implementation patterns for index options, GUC variables, scan operations, and cost estimation. The codebase uses established PostgreSQL patterns for reloptions and custom GUCs. Implementation requires:

1. Extending the `IvfflatOptions` and `HnswOptions` structs with new integer fields
2. Registering new reloptions via `add_int_reloption()` in the Init functions
3. Creating getter functions following existing patterns (`IvfflatGetLists`, `HnswGetM`)
4. Modifying scan functions to resolve effective search parameter
5. Updating cost estimation to use the resolved parameter
6. Using PostgreSQL's `find_option()` API to detect GUC source for user override detection

## Detailed Findings

### IVFFlat Index Option Infrastructure

The IVFFlat index option system is implemented across several files:

#### Option Struct Definition
**File**: `src/ivfflat.h:109-112`
```c
typedef struct IvfflatOptions
{
    int32       vl_len_;        /* varlena header (do not touch directly!) */
    int         lists;          /* number of lists */
}           IvfflatOptions;
```

The struct contains a varlena header followed by the current `lists` option. **To add `default_probes`, add a new `int` field after `lists`**.

#### Option Registration
**File**: `src/ivfflat.c:32-38`
```c
void
IvfflatInit(void)
{
    ivfflat_relopt_kind = add_reloption_kind();
    add_int_reloption(ivfflat_relopt_kind, "lists", "Number of inverted lists",
                      IVFFLAT_DEFAULT_LISTS, IVFFLAT_MIN_LISTS, IVFFLAT_MAX_LISTS, AccessExclusiveLock);
```

The `IvfflatInit()` function registers index options using `add_int_reloption()`. **To add `default_probes`, call `add_int_reloption()` here with:**
- Name: `"default_probes"`
- Description: `"Default number of probes for searches"`
- Default: `0` (sentinel for "unset")
- Min: `0` (allows unset)
- Max: `IVFFLAT_MAX_LISTS`
- Lock: `AccessExclusiveLock`

#### Option Parsing
**File**: `src/ivfflat.c:148-158`
```c
static bytea *
ivfflatoptions(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[] = {
        {"lists", RELOPT_TYPE_INT, offsetof(IvfflatOptions, lists)},
    };

    return (bytea *) build_reloptions(reloptions, validate,
                                      ivfflat_relopt_kind,
                                      sizeof(IvfflatOptions),
                                      tab, lengthof(tab));
}
```

**To add `default_probes`, add to the `tab` array:**
```c
{"default_probes", RELOPT_TYPE_INT, offsetof(IvfflatOptions, default_probes)},
```

#### Getter Pattern
**File**: `src/ivfutils.c:44-53`
```c
int
IvfflatGetLists(Relation index)
{
    IvfflatOptions *opts = (IvfflatOptions *) index->rd_options;

    if (opts)
        return opts->lists;

    return IVFFLAT_DEFAULT_LISTS;
}
```

**Create a similar getter for `default_probes`** that returns `0` (unset) when `rd_options` is NULL or when the option isn't set.

### HNSW Index Option Infrastructure

The HNSW index option system mirrors IVFFlat:

#### Option Struct Definition
**File**: `src/hnsw.h:188-192`
```c
typedef struct HnswOptions
{
    int32       vl_len_;        /* varlena header (do not touch directly!) */
    int         m;              /* number of connections */
    int         efConstruction; /* size of dynamic candidate list */
}           HnswOptions;
```

**To add `default_ef_search`, add a new `int` field after `efConstruction`**.

#### Option Registration
**File**: `src/hnsw.c:74-82`
```c
void
HnswInit(void)
{
    if (!process_shared_preload_libraries_in_progress)
        HnswInitLockTranche();

    hnsw_relopt_kind = add_reloption_kind();
    add_int_reloption(hnsw_relopt_kind, "m", "Max number of connections",
                      HNSW_DEFAULT_M, HNSW_MIN_M, HNSW_MAX_M, AccessExclusiveLock);
    add_int_reloption(hnsw_relopt_kind, "ef_construction", "Size of the dynamic candidate list for construction",
                      HNSW_DEFAULT_EF_CONSTRUCTION, HNSW_MIN_EF_CONSTRUCTION, HNSW_MAX_EF_CONSTRUCTION, AccessExclusiveLock);
```

**To add `default_ef_search`, call `add_int_reloption()` here with:**
- Name: `"default_ef_search"`
- Description: `"Default ef_search value for searches"`
- Default: `0` (sentinel for "unset")
- Min: `0` (allows unset)
- Max: `HNSW_MAX_EF_SEARCH`
- Lock: `AccessExclusiveLock`

#### Option Parsing
**File**: `src/hnsw.c:226-236`
```c
static bytea *
hnswoptions(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[] = {
        {"m", RELOPT_TYPE_INT, offsetof(HnswOptions, m)},
        {"ef_construction", RELOPT_TYPE_INT, offsetof(HnswOptions, efConstruction)},
    };

    return (bytea *) build_reloptions(reloptions, validate,
                                      hnsw_relopt_kind,
                                      sizeof(HnswOptions),
                                      tab, lengthof(tab));
}
```

**To add `default_ef_search`, add to the `tab` array:**
```c
{"default_ef_search", RELOPT_TYPE_INT, offsetof(HnswOptions, defaultEfSearch)},
```

#### Getter Pattern
**File**: `src/hnswutils.c:102-112`
```c
int
HnswGetM(Relation index)
{
    HnswOptions *opts = (HnswOptions *) index->rd_options;

    if (opts)
        return opts->m;

    return HNSW_DEFAULT_M;
}
```

**Create a similar getter for `default_ef_search`**.

### GUC Variable Registration

#### IVFFlat GUC Variables
**File**: `src/ivfflat.c:17-20` (declarations)
```c
int         ivfflat_probes;
int         ivfflat_iterative_scan;
int         ivfflat_max_probes;
static relopt_kind ivfflat_relopt_kind;
```

**File**: `src/ivfflat.c:39-42` (registration)
```c
DefineCustomIntVariable("ivfflat.probes", "Sets the number of probes",
                        "Valid range is 1..lists.", &ivfflat_probes,
                        IVFFLAT_DEFAULT_PROBES, IVFFLAT_MIN_LISTS, IVFFLAT_MAX_LISTS, PGC_USERSET, 0, NULL, NULL, NULL);
```

**File**: `src/ivfflat.h:46` (default constant)
```c
#define IVFFLAT_DEFAULT_PROBES  1
```

#### HNSW GUC Variables
**File**: `src/hnsw.c:27-31` (declarations)
```c
int         hnsw_ef_search;
int         hnsw_iterative_scan;
int         hnsw_max_scan_tuples;
double      hnsw_scan_mem_multiplier;
int         hnsw_lock_tranche_id;
static relopt_kind hnsw_relopt_kind;
```

**File**: `src/hnsw.c:83-85` (registration)
```c
DefineCustomIntVariable("hnsw.ef_search", "Sets the size of the dynamic candidate list for search",
                        "Valid range is 1..1000.", &hnsw_ef_search,
                        HNSW_DEFAULT_EF_SEARCH, HNSW_MIN_EF_SEARCH, HNSW_MAX_EF_SEARCH, PGC_USERSET, 0, NULL, NULL, NULL);
```

**File**: `src/hnsw.h:42` (default constant)
```c
#define HNSW_DEFAULT_EF_SEARCH  40
```

### Scan Functions - Where Search Parameters Are Used

#### IVFFlat Scan Entry
**File**: `src/ivfscan.c:244-297`
```c
IndexScanDesc
ivfflatbeginscan(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    IvfflatScanOpaque so;
    int         lists;
    int         dimensions;
    int         probes = ivfflat_probes;  // <-- Current: uses GUC directly
    int         maxProbes;
    // ...
    if (probes > lists)
        probes = lists;
    // ...
    so->probes = probes;
```

**Modification needed**: Replace `int probes = ivfflat_probes;` with logic that:
1. Reads index default via `IvfflatGetDefaultProbes(index)`
2. Checks if GUC source is `PGC_S_SESSION` (user explicitly set)
3. Uses GUC if explicitly set, otherwise uses index default (if non-zero), otherwise uses GUC default

#### HNSW Scan Entry
**File**: `src/hnswscan.c:14-43`
```c
static List *
GetScanItems(IndexScanDesc scan, Datum value)
{
    // ...
    return HnswSearchLayer(base, q, ep, hnsw_ef_search, 0, index, support, m, false, NULL, &so->v, hnsw_iterative_scan != HNSW_ITERATIVE_SCAN_OFF ? &so->discarded : NULL, true, &so->tuples);
}
```

The `hnsw_ef_search` GUC is passed directly to `HnswSearchLayer()`.

**File**: `src/hnswscan.c:117-140` (`hnswbeginscan`)
```c
IndexScanDesc
hnswbeginscan(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    HnswScanOpaque so;
    // ...
    // Note: ef_search not captured here, used directly in GetScanItems
```

**Modification needed**: The `GetScanItems` function (or a wrapper) needs to resolve the effective ef_search value using similar logic to IVFFlat.

### Cost Estimation Functions

#### IVFFlat Cost Estimation
**File**: `src/ivfflat.c:76-138`
```c
static void
ivfflatcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                    Cost *indexStartupCost, Cost *indexTotalCost,
                    Selectivity *indexSelectivity, double *indexCorrelation,
                    double *indexPages)
{
    // ...
    index = index_open(path->indexinfo->indexoid, NoLock);
    IvfflatGetMetaPageInfo(index, &lists, NULL);
    index_close(index, NoLock);

    /* Get the ratio of lists that we need to visit */
    ratio = ((double) ivfflat_probes) / lists;  // <-- Uses GUC directly
```

**Modification needed**: Replace `ivfflat_probes` with resolved effective value (considering index default and GUC source).

#### HNSW Cost Estimation
**File**: `src/hnsw.c:119-203`
```c
static void
hnswcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                 Cost *indexStartupCost, Cost *indexTotalCost,
                 Selectivity *indexSelectivity, double *indexCorrelation,
                 double *indexPages)
{
    // ...
    index = index_open(path->indexinfo->indexoid, NoLock);
    HnswGetMetaPageInfo(index, &m, NULL);
    index_close(index, NoLock);
    // ...
    int         layer0TuplesMax = HnswGetLayerM(m, 0) * hnsw_ef_search;  // <-- Uses GUC directly
```

**Modification needed**: Replace `hnsw_ef_search` with resolved effective value.

### GUC Source Detection

PostgreSQL provides the `find_option()` function to access GUC metadata:

**PostgreSQL API** (from `src/include/utils/guc_tables.h:312-316`):
```c
extern struct config_generic *find_option(const char *name,
                                          bool create_placeholders,
                                          bool skip_errors,
                                          int elevel);
```

The `config_generic` structure contains a `source` field of type `GucSource`:

**PostgreSQL API** (from `src/include/utils/guc.h:104-127`):
```c
typedef enum
{
    PGC_S_DEFAULT,              /* hard-wired default ("boot_val") */
    PGC_S_DYNAMIC_DEFAULT,      /* default computed during initialization */
    PGC_S_ENV_VAR,              /* postmaster environment variable */
    PGC_S_FILE,                 /* postgresql.conf */
    PGC_S_ARGV,                 /* postmaster command line */
    PGC_S_GLOBAL,               /* global in-database setting */
    PGC_S_DATABASE,             /* per-database setting */
    PGC_S_USER,                 /* per-user setting */
    PGC_S_DATABASE_USER,        /* per-user-and-database setting */
    PGC_S_CLIENT,               /* from client connection request */
    PGC_S_OVERRIDE,             /* special case to forcibly set default */
    PGC_S_INTERACTIVE,          /* dividing line for error reporting */
    PGC_S_TEST,                 /* test per-database or per-user setting */
    PGC_S_SESSION,              /* SET command */
} GucSource;
```

**Implementation pattern for detecting user SET**:
```c
#include "utils/guc_tables.h"

static bool
IsGucSetByUser(const char *guc_name)
{
    struct config_generic *record;
    
    record = find_option(guc_name, false, true, ERROR);
    if (record == NULL)
        return false;
    
    return record->source == PGC_S_SESSION;
}
```

This can be used to implement the precedence logic:
1. If `IsGucSetByUser("ivfflat.probes")` returns true → use `ivfflat_probes`
2. Else if index has `default_probes > 0` → use index default
3. Else → use `ivfflat_probes` (which will be the system default)

### SQL Migration Pattern

Index options don't require explicit SQL migration - they're automatically available once registered in C code via `add_int_reloption()`. The options become usable in `CREATE INDEX ... WITH (option = value)` and `ALTER INDEX ... SET (option = value)` syntax.

**Example from existing tests** (`test/sql/ivfflat_vector.sql:75-78`):
```sql
CREATE TABLE t (val vector(3));
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 0);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 32769);
```

### Test Patterns

#### IVFFlat Test Pattern
**File**: `test/sql/ivfflat_vector.sql:75-95`
```sql
-- options

CREATE TABLE t (val vector(3));
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 0);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 32769);

SHOW ivfflat.probes;

SET ivfflat.probes = 0;
SET ivfflat.probes = 32769;
```

**Tests to add for `default_probes`**:
1. Create index with `default_probes` option
2. Query using index, verify probes used matches default
3. SET `ivfflat.probes`, query, verify GUC value takes precedence
4. RESET `ivfflat.probes`, query, verify index default is used again
5. ALTER INDEX to change `default_probes`, verify new default is used
6. Test `default_probes = 0` falls back to GUC default

#### HNSW Test Pattern
**File**: `test/sql/hnsw_vector.sql:86-117`
```sql
-- options

CREATE TABLE t (val vector(3));
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 1);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 101);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (ef_construction = 3);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (ef_construction = 1001);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 16, ef_construction = 31);

SHOW hnsw.ef_search;

SET hnsw.ef_search = 0;
SET hnsw.ef_search = 1001;
```

**Tests to add for `default_ef_search`**:
1. Create index with `default_ef_search` option
2. Query using index, verify ef_search used matches default
3. SET `hnsw.ef_search`, query, verify GUC value takes precedence
4. RESET `hnsw.ef_search`, query, verify index default is used again
5. ALTER INDEX to change `default_ef_search`, verify new default is used
6. Test `default_ef_search = 0` falls back to GUC default

## Code References

### IVFFlat Implementation Files
- `src/ivfflat.h:109-112` - IvfflatOptions struct definition
- `src/ivfflat.h:46` - IVFFLAT_DEFAULT_PROBES constant
- `src/ivfflat.h:84` - extern declaration for ivfflat_probes GUC
- `src/ivfflat.c:17-20` - GUC variable definitions
- `src/ivfflat.c:32-52` - IvfflatInit() option and GUC registration
- `src/ivfflat.c:76-138` - ivfflatcostestimate() cost estimation
- `src/ivfflat.c:148-158` - ivfflatoptions() reloption parsing
- `src/ivfscan.c:244-297` - ivfflatbeginscan() scan initialization
- `src/ivfutils.c:44-53` - IvfflatGetLists() getter pattern

### HNSW Implementation Files
- `src/hnsw.h:188-192` - HnswOptions struct definition
- `src/hnsw.h:42` - HNSW_DEFAULT_EF_SEARCH constant
- `src/hnsw.h:112` - extern declaration for hnsw_ef_search GUC
- `src/hnsw.c:27-31` - GUC variable definitions
- `src/hnsw.c:74-98` - HnswInit() option and GUC registration
- `src/hnsw.c:119-203` - hnswcostestimate() cost estimation
- `src/hnsw.c:226-236` - hnswoptions() reloption parsing
- `src/hnswscan.c:14-43` - GetScanItems() where ef_search is used
- `src/hnswscan.c:117-140` - hnswbeginscan() scan initialization
- `src/hnswutils.c:102-112` - HnswGetM() getter pattern

### Test Files
- `test/sql/ivfflat_vector.sql:75-95` - IVFFlat option tests
- `test/sql/hnsw_vector.sql:86-117` - HNSW option tests

## Architecture Documentation

### Option Resolution Pattern

The existing pattern for reading index options:

1. **At scan initiation**: The `beginscan` function receives `Relation index`
2. **Access options**: Cast `index->rd_options` to the options struct pointer
3. **Handle NULL**: If `rd_options` is NULL (no custom options), use default
4. **Return value**: Return option value or default

### Proposed Effective Value Resolution

New pattern for resolving effective search parameter:

```
GetEffectiveProbes(Relation index):
    1. index_default = IvfflatGetDefaultProbes(index)
    2. guc_explicitly_set = IsGucSetByUser("ivfflat.probes")
    3. if guc_explicitly_set:
           return ivfflat_probes  // User's explicit choice
    4. if index_default > 0:
           return index_default   // Index-specific default
    5. return ivfflat_probes      // System/session default
```

Same pattern applies for HNSW with `hnsw.ef_search`.

### Files Requiring Modification

1. **src/ivfflat.h** - Add `default_probes` to IvfflatOptions struct
2. **src/ivfflat.c** - Register `default_probes` reloption, add to parsing table
3. **src/ivfutils.c** - Add `IvfflatGetDefaultProbes()` getter
4. **src/ivfscan.c** - Modify `ivfflatbeginscan()` to use effective probes
5. **src/ivfflat.c** - Modify `ivfflatcostestimate()` to use effective probes

6. **src/hnsw.h** - Add `defaultEfSearch` to HnswOptions struct
7. **src/hnsw.c** - Register `default_ef_search` reloption, add to parsing table
8. **src/hnswutils.c** - Add `HnswGetDefaultEfSearch()` getter
9. **src/hnswscan.c** - Modify to use effective ef_search
10. **src/hnsw.c** - Modify `hnswcostestimate()` to use effective ef_search

11. **test/sql/ivfflat_vector.sql** - Add tests for default_probes
12. **test/sql/hnsw_vector.sql** - Add tests for default_ef_search
13. **test/expected/ivfflat_vector.out** - Expected output
14. **test/expected/hnsw_vector.out** - Expected output

## Open Questions

1. **Version compatibility**: Need to verify `find_option()` and `GucSource` API availability across PostgreSQL 12-17. The SpecResearch.md notes this as a potential risk.

2. **Performance**: Is there measurable overhead from calling `find_option()` on every scan? Consider caching the check result within the scan context if needed.

3. **Thread safety**: Ensure `find_option()` is safe to call from index scan functions (it should be, as it's used widely in PostgreSQL).
