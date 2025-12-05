---
agent: 'PAW-01B Spec Researcher'
---
# Spec Research Prompt: Search Defaults Index Option

Perform research to answer the following questions.

Target Branch: feature/235-search-defaults-index-option
Issue URL: https://github.com/pgvector/pgvector/issues/235
Additional Inputs: none

## Questions

1. How does PostgreSQL's GUC source tracking work? Specifically, how can extension code determine whether a GUC was set via `PGC_S_SESSION` (explicit user SET in the current session) versus other sources like `PGC_S_DEFAULT`, `PGC_S_DATABASE`, `PGC_S_FILE`?

2. How are index options (reloptions) accessed during index scans in pgvector? What is the code path from `ivfflatbeginscan`/`hnswbeginscan` to reading the options stored with the index?

3. How does PostgreSQL handle index options that need to be read during scan time (not just build time)? What functions or patterns are used to retrieve reloptions from an open index relation?

4. Is there any existing pattern in pgvector or PostgreSQL for index options that have a NULL/unset state meaning "use a GUC default"? How is this typically represented (0, -1, or explicit NULL marker)?

5. How is the `IvfflatOptions`/`HnswOptions` struct populated and accessed? Where is `index->rd_options` set and when is it available during scan operations?

6. What happens during query planning (cost estimation) when an index option value needs to be read? The cost estimate functions (`ivfflatcostestimate`, `hnswcostestimate`) currently use the GUC value directly - how would they need to change to consider per-index defaults?

### Optional External / Context

1. Are there any PostgreSQL extension precedents for per-index search parameter defaults that override session-level GUCs?

2. What is the PostgreSQL community guidance (if any) on using index options vs GUCs for search-time parameters?
