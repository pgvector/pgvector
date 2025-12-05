# Spec Research: Search Defaults Index Option

**Work ID**: 235-search-defaults-index-option  
**Date**: 2024-12-04

## Summary

This document captures research findings on the current pgvector system behavior relevant to implementing per-index default search parameters. The key findings confirm that: (1) PostgreSQL's GUC infrastructure tracks the source of configuration values, enabling detection of explicit user SET commands via `PGC_S_SESSION`; (2) index options (reloptions) are accessible during scans via `index->rd_options`; and (3) pgvector already has established patterns for reading index options that can be extended for new search defaults.

---

## Research Findings

### Question 1: How does PostgreSQL's GUC source tracking work?

**Question**: How can extension code determine whether a GUC was set via `PGC_S_SESSION` (explicit user SET in the current session) versus other sources like `PGC_S_DEFAULT`, `PGC_S_DATABASE`, `PGC_S_FILE`?

**Answer**: PostgreSQL tracks the source of each GUC value through the `GucSource` enum defined in `guc.h`. Each GUC variable has a `source` field in its `config_generic` structure that records how the current value was set. The `PGC_S_SESSION` value (126 in the enum) specifically indicates the value was set via a SET command in the current session.

The GucSource enum hierarchy (from lowest to highest priority):
- `PGC_S_DEFAULT` (113) - hard-wired default ("boot_val")
- `PGC_S_DYNAMIC_DEFAULT` (114) - computed during initialization
- `PGC_S_ENV_VAR` (115) - postmaster environment variable
- `PGC_S_FILE` (116) - postgresql.conf
- `PGC_S_ARGV` (117) - postmaster command line
- `PGC_S_GLOBAL` (118) - global in-database setting
- `PGC_S_DATABASE` (119) - per-database setting
- `PGC_S_USER` (120) - per-user setting
- `PGC_S_DATABASE_USER` (121) - per-user-and-database setting
- `PGC_S_CLIENT` (122) - from client connection request
- `PGC_S_OVERRIDE` (123) - special case to forcibly set default
- `PGC_S_INTERACTIVE` (124) - dividing line for error reporting
- `PGC_S_TEST` (125) - test per-database or per-user setting
- `PGC_S_SESSION` (126) - SET command

To detect if a GUC was explicitly set in the session, extension code can use `find_option()` to get the `config_generic*` structure, then check `record->source >= PGC_S_INTERACTIVE` (values >= INTERACTIVE are considered user-interactive changes, including SESSION). More specifically, checking `record->source == PGC_S_SESSION` identifies explicit SET commands.

**Evidence**: PostgreSQL guc.h header documentation, GucSource enum definition (lines 111-127 of guc.h)

**Implications**: The feature can reliably detect user intent by checking whether `ivfflat.probes` or `hnsw.ef_search` has source `PGC_S_SESSION`. This aligns with the RFC proposal.

---

### Question 2: How are index options accessed during index scans in pgvector?

**Question**: What is the code path from `ivfflatbeginscan`/`hnswbeginscan` to reading the options stored with the index?

**Answer**: During index scans, pgvector can access index options through the `Relation index` parameter passed to scan functions. The options are available via `index->rd_options` which is a pointer to the parsed reloptions structure.

In `ivfflatbeginscan()` (ivfscan.c:244-296):
1. Function receives `Relation index` parameter
2. Calls `IvfflatGetMetaPageInfo(index, &lists, &dimensions)` to get metadata
3. Uses the global GUC variable `ivfflat_probes` directly for probe count

In `hnswbeginscan()` (hnswscan.c:117-150):
1. Function receives `Relation index` parameter  
2. Uses global GUC variable `hnsw_ef_search` via `GetScanItems()` (line 43)

Currently, neither function reads options from `index->rd_options` for search parameters—they rely solely on GUC variables.

**Evidence**: ivfscan.c (ivfflatbeginscan), hnswscan.c (hnswbeginscan, GetScanItems)

**Implications**: The beginscan functions have access to the index relation and can read `rd_options` if new search default options are added.

---

### Question 3: How does PostgreSQL handle index options read during scan time?

**Question**: What functions or patterns are used to retrieve reloptions from an open index relation?

**Answer**: PostgreSQL provides `rd_options` on the `RelationData` structure (the index relation). This field contains parsed reloptions and is available whenever the relation is open.

pgvector already demonstrates this pattern in utility functions:

```c
// From hnswutils.c:113
int HnswGetM(Relation index) {
    HnswOptions *opts = (HnswOptions *) index->rd_options;
    if (opts)
        return opts->m;
    return HNSW_DEFAULT_M;
}

// From ivfutils.c:47
int IvfflatGetLists(Relation index) {
    IvfflatOptions *opts = (IvfflatOptions *) index->rd_options;
    if (opts)
        return opts->lists;
    return IVFFLAT_DEFAULT_LISTS;
}
```

The pattern is:
1. Cast `index->rd_options` to the appropriate options struct pointer
2. Check if opts is non-NULL
3. Return the option value, or fall back to a default if NULL

**Evidence**: hnswutils.c (HnswGetM, HnswGetEfConstruction), ivfutils.c (IvfflatGetLists)

**Implications**: New getter functions like `IvfflatGetDefaultProbes()` and `HnswGetDefaultEfSearch()` can follow this established pattern.

---

### Question 4: Pattern for NULL/unset index options meaning "use GUC default"

**Question**: Is there any existing pattern in pgvector or PostgreSQL for index options that have a NULL/unset state meaning "use a GUC default"? How is this typically represented?

**Answer**: pgvector currently handles this through the `rd_options` pointer being NULL. When no custom options are specified at index creation, `rd_options` can be NULL, and the getter functions return a hardcoded default value.

For integer reloptions, PostgreSQL's `add_int_reloption()` allows specifying a default value. Typically, 0 or -1 are used as sentinel values meaning "unset" for optional integer parameters, though this depends on whether 0 is a valid value for the parameter.

For the proposed `default_probes` and `default_ef_search`:
- Value 0 can serve as "unset" since 0 probes/ef_search is invalid (minimum is 1)
- Alternatively, the `rd_options` pattern already handles NULL gracefully

**Evidence**: IvfflatGetLists pattern, HnswGetM pattern, PostgreSQL reloptions framework

**Implications**: Using 0 as the "unset" sentinel is appropriate since valid values start at 1. The implementation should interpret 0 as "no index default specified, use GUC value."

---

### Question 5: How is the Options struct populated and accessed?

**Question**: How is the `IvfflatOptions`/`HnswOptions` struct populated and accessed? Where is `index->rd_options` set?

**Answer**: The options structs are defined in the header files:

```c
// ivfflat.h:116-120
typedef struct IvfflatOptions {
    int32   vl_len_;    /* varlena header */
    int     lists;      /* number of lists */
} IvfflatOptions;

// hnsw.h:181-186
typedef struct HnswOptions {
    int32   vl_len_;    /* varlena header */
    int     m;          /* number of connections */
    int     efConstruction;
} HnswOptions;
```

Population happens through the `amoptions` callback registered in the index handler:

1. **ivfflatoptions()** (ivfflat.c:152-161) - parses reloptions using `build_reloptions()`
2. **hnswoptions()** (hnsw.c:232-242) - parses reloptions using `build_reloptions()`

These functions use `relopt_parse_elt` arrays to map option names to struct offsets:
```c
static const relopt_parse_elt tab[] = {
    {"lists", RELOPT_TYPE_INT, offsetof(IvfflatOptions, lists)},
};
```

The options are registered during initialization in `IvfflatInit()` and `HnswInit()` via `add_int_reloption()`.

PostgreSQL sets `rd_options` when opening the relation, calling the amoptions function to parse the stored reloptions.

**Evidence**: ivfflat.c (ivfflatoptions, IvfflatInit), hnsw.c (hnswoptions, HnswInit), ivfflat.h, hnsw.h

**Implications**: To add new options:
1. Extend the options struct with new integer fields
2. Register options with `add_int_reloption()` in Init functions
3. Add entries to the `relopt_parse_elt` array in the options function
4. Create getter functions following existing patterns

---

### Question 6: Index options during cost estimation

**Question**: What happens during query planning (cost estimation) when an index option value needs to be read? How would cost functions need to change?

**Answer**: The cost estimation functions (`ivfflatcostestimate`, `hnswcostestimate`) currently use GUC values directly:

In **ivfflatcostestimate()** (ivfflat.c:78-138):
```c
index = index_open(path->indexinfo->indexoid, NoLock);
IvfflatGetMetaPageInfo(index, &lists, NULL);
index_close(index, NoLock);

ratio = ((double) ivfflat_probes) / lists;  // Uses GUC directly
```

In **hnswcostestimate()** (hnsw.c:119-203):
```c
index = index_open(path->indexinfo->indexoid, NoLock);
HnswGetMetaPageInfo(index, &m, NULL);
index_close(index, NoLock);

int layer0TuplesMax = HnswGetLayerM(m, 0) * hnsw_ef_search;  // Uses GUC directly
```

Both functions already open the index relation to read metadata. The `rd_options` field is available on the opened relation.

**Evidence**: ivfflat.c (ivfflatcostestimate lines 104-105, 111), hnsw.c (hnswcostestimate lines 157-158, 181)

**Implications**: Cost estimation functions can be updated to:
1. Read the index default from `index->rd_options`
2. Check if GUC source is `PGC_S_SESSION`
3. Use index default if set and GUC not explicitly set; otherwise use GUC value
4. The index relation is already opened, so accessing `rd_options` adds minimal overhead

---

## Open Unknowns

1. **GUC source API stability across PostgreSQL versions**: The `config_generic` struct and `find_option()` function need to be verified for availability across all PostgreSQL versions pgvector supports (12+). If the API differs, version-specific handling may be needed.

2. **SET LOCAL behavior**: The spec assumes `SET LOCAL` should also take precedence over index defaults (treated same as `PGC_S_SESSION`). Need to verify this is the desired behavior and that SET LOCAL uses the same source indicator.

The Spec Agent will review these with you. You may provide answers here if possible.

---

## User-Provided External Knowledge (Manual Fill)

The following optional external/context questions require input outside the codebase:

- [ ] Are there any PostgreSQL extension precedents for per-index search parameter defaults that override session-level GUCs?

- [ ] What is the PostgreSQL community guidance (if any) on using index options vs GUCs for search-time parameters?
