# Feature Specification: Search Defaults Index Option

**Branch**: feature/235-search-defaults-index-option  |  **Created**: 2024-12-04  |  **Status**: Final
**Input Brief**: Add per-index default values for search parameters (probes/ef_search) that override session-level GUC defaults while respecting explicit user SET commands.

## Overview

Database administrators and application developers who use pgvector's approximate nearest neighbor indexes often need different search accuracy settings for different tables. Currently, configuring search parameters like the number of IVFFlat probes or HNSW ef_search requires setting session-level configuration variables before each query or using database-wide defaults—neither approach works well when a single database contains multiple vector indexes with distinct performance/accuracy tradeoffs.

Consider a scenario where an e-commerce platform stores product embeddings in one table (optimized for speed with fewer probes) and customer support ticket embeddings in another (requiring higher accuracy with more probes). Today, developers must wrap each query in transaction blocks with SET LOCAL statements, introducing complexity and potential race conditions when connections are shared. With per-index defaults, administrators can specify the optimal search parameters once at index creation time, and queries automatically use the appropriate settings based on which index is selected.

The feature introduces new index options that allow specifying default search parameters when creating or altering vector indexes. These defaults take effect when queries use the index, eliminating the need for application code to manage parameter settings. Importantly, the feature preserves backward compatibility: if a user explicitly sets a session-level parameter before a query, that explicit choice takes precedence over the index default, maintaining user control over individual query behavior.

This approach solves several real-world challenges reported by pgvector users: managing different probes/ef_search values across partitioned tables, simplifying application code that previously needed to track per-table settings, and enabling non-superuser accounts to effectively configure vector search behavior without ALTER DATABASE permissions.

## Objectives

- Enable database administrators to specify default search parameters at index creation time (Rationale: reduces per-query configuration burden and prevents parameter management complexity in application code)
- Preserve explicit user intent when session-level parameters are set (Rationale: users who explicitly SET a parameter expect that value to be used, regardless of index defaults)
- Maintain full backward compatibility with existing indexes and queries (Rationale: existing deployments must continue working without modification)
- Support different default values across multiple indexes in the same database (Rationale: addresses the partitioned data and multi-table use cases driving this feature request)
- Allow updating index defaults after creation without rebuilding (Rationale: enables tuning search parameters as data characteristics evolve)

## User Scenarios & Testing

### User Story P1 – Configure Index with Search Default

Narrative: An administrator creates a new vector index and specifies the default number of probes/ef_search to use during searches. When users query the table, the index automatically uses the configured default without requiring any SET commands.

Independent Test: Query a table with the new index and verify the search uses the index-specified default parameter value.

Acceptance Scenarios:
1. Given a table with vector data, When creating an IVFFlat index with `default_probes = 10`, Then queries using that index probe 10 lists by default
2. Given a table with vector data, When creating an HNSW index with `default_ef_search = 100`, Then queries using that index use ef_search of 100 by default
3. Given an index with default search parameters, When a user queries without setting session parameters, Then the query uses the index default rather than the system default

### User Story P2 – Explicit Session Setting Overrides Index Default

Narrative: A user needs to run a specific query with different search parameters than the index default. They issue a SET command for the search parameter, and the query respects their explicit choice rather than using the index default.

Independent Test: SET a session parameter, query an index with a different default, and verify the session value is used.

Acceptance Scenarios:
1. Given an index with `default_probes = 5`, When user runs `SET ivfflat.probes = 20` then queries, Then the query uses 20 probes (not 5)
2. Given an index with `default_ef_search = 50`, When user runs `SET LOCAL hnsw.ef_search = 200` then queries within the transaction, Then the query uses ef_search of 200
3. Given a session with explicitly set parameter, When the parameter is RESET to default, Then subsequent queries use the index default (if set)

### User Story P3 – Multiple Indexes with Different Defaults

Narrative: A database has multiple tables with vector indexes, each configured with different search parameter defaults appropriate for their data characteristics. Queries against each table automatically use the correct settings.

Independent Test: Query two different tables with indexes having different defaults and verify each uses its own default.

Acceptance Scenarios:
1. Given table A with index default_probes=5 and table B with default_probes=20, When querying table A without session SET, Then 5 probes are used
2. Given table A with index default_probes=5 and table B with default_probes=20, When querying table B without session SET, Then 20 probes are used
3. Given a partitioned table with per-partition indexes having different defaults, When queries hit different partitions, Then each uses its partition's index default

### User Story P4 – Modify Index Default After Creation

Narrative: An administrator realizes the initially configured search default is not optimal and wants to change it without rebuilding the index.

Independent Test: Use ALTER INDEX to change the default and verify subsequent queries use the new value.

Acceptance Scenarios:
1. Given an existing IVFFlat index without default_probes, When `ALTER INDEX ... SET (default_probes = 15)` is run, Then subsequent queries use 15 probes
2. Given an existing HNSW index with default_ef_search=50, When `ALTER INDEX ... SET (default_ef_search = 100)` is run, Then subsequent queries use ef_search of 100
3. Given an index with a configured default, When `ALTER INDEX ... RESET (default_probes)` is run, Then subsequent queries fall back to the GUC value

### Edge Cases

- **EC-001**: Index created with default value of 0: Should be treated as "unset" and fall back to GUC value.
- **EC-002**: default_probes exceeds number of lists: Should be clamped to list count (existing behavior preserved).
- **EC-003**: default_ef_search exceeds maximum: Should be clamped to valid range.
- **EC-004**: Index accessed during query planning (cost estimation): Must read index default for accurate cost estimates.
- **EC-005**: Concurrent sessions with different explicit SET values: Each session's explicit SET takes precedence for that session only.
- **EC-006**: RESET command after explicit SET: Subsequent queries should revert to using index default if set, otherwise GUC default.

## Requirements

### Functional Requirements

- FR-001: System shall accept `default_probes` as an integer index option when creating or altering IVFFlat indexes (Stories: P1, P4)
- FR-002: System shall accept `default_ef_search` as an integer index option when creating or altering HNSW indexes (Stories: P1, P4)
- FR-003: When an index has a non-null default search parameter and no explicit session SET has occurred, queries shall use the index default (Stories: P1, P3)
- FR-004: When a user explicitly sets the corresponding GUC in their session, queries shall use the session value regardless of index defaults (Stories: P2)
- FR-005: When no index default is set (null/zero), queries shall use the current GUC value (existing behavior) (Stories: P1)
- FR-006: Index cost estimation shall consider per-index defaults when calculating scan costs (Stories: P1, P3)
- FR-007: System shall allow modifying index defaults via ALTER INDEX without requiring index rebuild (Stories: P4)

### Key Entities

- Index Options: Extended to include optional default search parameters stored with the index metadata
- GUC Configuration: Existing session-level variables whose source is tracked to determine explicit user intent
- Index Scan Context: Runtime context that resolves effective search parameter from index default vs GUC

### Cross-Cutting / Non-Functional

- Backward Compatibility: Existing indexes without new options must behave identically to current behavior
- Performance: Reading index default during scan initiation must not add measurable overhead compared to GUC access
- PostgreSQL Version Compatibility: Must work with the same PostgreSQL versions pgvector currently supports

## Success Criteria

- **SC-001**: IVFFlat index created with `default_probes = N` uses N probes during search when no session SET is active. (FR-001, FR-003)
- **SC-002**: HNSW index created with `default_ef_search = N` uses ef_search of N during search when no session SET is active. (FR-002, FR-003)
- **SC-003**: Explicit `SET ivfflat.probes` or `SET hnsw.ef_search` in session overrides any index default. (FR-004)
- **SC-004**: Indexes created without new options behave identically to current behavior (use GUC values). (FR-005)
- **SC-005**: Query planner cost estimates reflect per-index defaults when applicable. (FR-006)
- **SC-006**: ALTER INDEX can add, modify, or remove default search parameters without index rebuild. (FR-007)
- **SC-007**: Index default value of 0 is treated as "unset" and falls back to GUC value. (EC-001)
- **SC-008**: SET LOCAL within a transaction overrides index defaults for queries in that transaction. (FR-004)

## Assumptions

- **A-001**: PostgreSQL's GUC infrastructure tracks the source of configuration values via the `GucSource` enum. Extension code can detect explicit session SET commands by checking if the source is `PGC_S_SESSION` (value 126). Research confirms this mechanism is available and stable.
- **A-002**: Index reloptions are accessible during scan operations via `index->rd_options`. Research confirms pgvector already uses this pattern for existing options like `lists`, `m`, and `efConstruction`.
- **A-003**: The value 0 serves as the "unset" sentinel for the new integer options. Since valid values for probes and ef_search start at 1, zero unambiguously means "no index default specified."
- **A-004**: The overhead of checking GUC source during scan initiation is negligible compared to the actual search operation, which involves disk I/O and distance calculations.
- **A-005**: SET LOCAL within a transaction sets the GUC source to `PGC_S_SESSION`, same as a regular SET command. Both are treated as explicit user overrides that take precedence over index defaults.

## Scope

In Scope:
- New `default_probes` index option for IVFFlat
- New `default_ef_search` index option for HNSW
- Logic to resolve effective search parameter (index default vs GUC)
- GUC source detection to identify explicit session SET
- Integration with ALTER INDEX for modifying defaults
- Cost estimation updates to use resolved parameter value

Out of Scope:
- "Smart" automatic defaults based on data characteristics (mentioned in issue as separate future work)
- Query hint syntax like `SELECT ... WITH (ivfflat.probes=10)` (requires PostgreSQL parser changes, not possible in extension)
- Per-query parameter overrides without SET command
- Default values for other search parameters (iterative_scan, max_probes, etc.)

## Dependencies

- PostgreSQL GUC infrastructure (source tracking via GetConfigOptionByNum or similar)
- PostgreSQL reloptions framework for index option handling
- Existing pgvector index access method infrastructure

## Risks & Mitigations

- **Risk**: GUC source tracking API may differ across PostgreSQL versions. **Mitigation**: Research indicates the `GucSource` enum and `find_option()` function have been stable in PostgreSQL. Version-specific compatibility checks should be added during implementation to verify behavior across all supported versions (12+).
- **Risk**: Index options not accessible during cost estimation phase. **Mitigation**: Research confirms cost estimation functions already open the index relation (to read metadata like list count); `rd_options` is available on the opened relation with minimal additional overhead.
- **Risk**: Complexity in resolving "which value to use" logic could introduce bugs. **Mitigation**: Clear precedence rules (explicit SET > index default > GUC default) with comprehensive test coverage for all scenarios.
- **Risk**: Users may be confused when queries use different parameters than they expect. **Mitigation**: Clear documentation explaining the precedence hierarchy with examples. Consider future enhancement to show parameter source in EXPLAIN output.

## References

- Issue: https://github.com/pgvector/pgvector/issues/235
- Research: .paw/work/235-search-defaults-index-option/SpecResearch.md
- External: PostgreSQL GUC source tracking: https://github.com/postgres/postgres/blob/master/src/include/utils/guc.h#L78-L123

## Glossary

- GUC: Grand Unified Configuration - PostgreSQL's configuration parameter system
- probes: Number of IVFFlat inverted lists to search during a query
- ef_search: Size of the dynamic candidate list during HNSW search
- PGC_S_SESSION: GUC source indicator meaning the value was set explicitly in the current session
