#include "postgres.h"

#include "ann.h"
#include "catalog/namespace.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/tuplestore.h"
#include "vector.h"

#define ANN_OP_LOAD_INDEX 11
#define ANN_OP_INDEX_SEARCH_PACKED 14
#define ANN_OP_INDEX_INFO 15
#define ANN_INDEX_NAME_MAX 63
#define ANN_INDEX_MAX_K 10000
#define ANN_LOAD_TIMEOUT_MS (30 * 60 * 1000)
#define ANN_KEY_FETCH 65536

/* Statuses shared with ann/backend.h. */
#define ANN_STATUS_OK 0
#define ANN_STATUS_INVALID 1
#define ANN_STATUS_UNSUPPORTED 2
#define ANN_STATUS_STALE 3
#define ANN_STATUS_OVERLOADED 7

/* What the worker reports for a loaded index. */
typedef struct AnnIndexInfo
{
	uint64		rows;
	char		source[256];
	char		key[64];
	char		vector[64];
} AnnIndexInfo;

/* Catalog row: where an index's vectors live on this host. */
typedef struct AnnCatalogEntry
{
	char	   *source;			/* quoted relation name */
	char	   *key;			/* quoted column names */
	char	   *vector;
} AnnCatalogEntry;

static void
Put32(StringInfo buffer, uint32 value)
{
	appendBinaryStringInfo(buffer, (char *) &value, sizeof(value));
}

static const char *
CheckName(text *name)
{
	char	   *value = text_to_cstring(name);

	if (strlen(value) == 0 || strlen(value) > ANN_INDEX_NAME_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ann index name must have 1 to %d bytes", ANN_INDEX_NAME_MAX)));
	return value;
}

/* Networked GPU nodes when configured, otherwise the local worker socket. */
static bool
IndexCall(uint16 operation, const char *payload, size_t length, int timeoutMs,
		  uint32 *status, char **reply, uint32 *replyLength)
{
	if (AnnRemoteConfigured())
		return AnnRemoteCall(operation, payload, length, timeoutMs, status, reply, replyLength);
	return AnnWorkerCall(operation, payload, length, timeoutMs, status, reply, replyLength);
}

static void
CheckWorker(bool ok, uint32 status, const char *name)
{
	if (!ok)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("ann worker is unavailable"),
				 errhint("Set ann.worker_address or ann.worker_socket to a running ann-worker.")));
	if (status == ANN_STATUS_OK)
		return;
	if (status == ANN_STATUS_OVERLOADED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("ann worker is overloaded")));
	if (status == ANN_STATUS_STALE)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("ann index \"%s\" is not loaded, or is already loaded", name)));
	if (status == ANN_STATUS_UNSUPPORTED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ann worker backend does not serve loaded indexes")));
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("ann worker rejected the request for index \"%s\" (status %u)", name, status)));
}

static void
ParseInfo(const char *reply, AnnIndexInfo *info)
{
	unsigned long long rows;

	strcpy(info->key, "-");
	strcpy(info->vector, "-");
	if (sscanf(reply, "rows %llu dimensions %*u shards %*u dtype %*s source %255s key %63s vector %63s",
			   &rows, info->source, info->key, info->vector) < 2)
		elog(ERROR, "unexpected ann worker reply: %s", reply);
	info->rows = rows;
}

/* Schema-qualified catalog name, in the extension's schema. */
static char *
CatalogName(FunctionCallInfo fcinfo)
{
	char	   *schema = get_namespace_name(get_func_namespace(fcinfo->flinfo->fn_oid));

	return psprintf("%s.ann_index_catalog", quote_identifier(schema));
}

static char *
QualifiedSource(const char *source)
{
	List	   *names = stringToQualifiedNameList(source
#if PG_VERSION_NUM >= 160000
												  ,NULL
#endif
		);

	return NameListToQuotedString(names);
}

/*
 * The index is immutable: refuse one whose source table on this host no
 * longer has the row count it was built from. Rebuild after changing the
 * table.
 */
static void
CheckSource(const AnnIndexInfo *info)
{
	char	   *query;
	bool		isnull;
	int64		count;

	if (strcmp(info->source, "-") == 0)
		return;
	query = psprintf("SELECT count(*) FROM %s", QualifiedSource(info->source));
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	if (SPI_execute(query, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "could not count rows of %s", info->source);
	count = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	SPI_finish();
	if (count != (int64) info->rows)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("ann index is stale: %s has " INT64_FORMAT " rows, index has " UINT64_FORMAT,
						info->source, count, info->rows),
				 errhint("Rebuild the index with scripts/ann/build_cuvs_index.py.")));
}

/* Record where the index's rows live, so this host can route exact scans. */
static void
RecordCatalog(FunctionCallInfo fcinfo, const char *name, const AnnIndexInfo *info)
{
	Oid			types[5] = {TEXTOID, TEXTOID, TEXTOID, TEXTOID, INT8OID};
	Datum		values[5];

	if (strcmp(info->source, "-") == 0 || strcmp(info->key, "-") == 0 || strcmp(info->vector, "-") == 0)
		return;
	values[0] = CStringGetTextDatum(name);
	values[1] = CStringGetTextDatum(QualifiedSource(info->source));
	values[2] = CStringGetTextDatum(info->key);
	values[3] = CStringGetTextDatum(info->vector);
	values[4] = Int64GetDatum((int64) info->rows);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	if (SPI_execute_with_args(psprintf("INSERT INTO %s (name, source, key_column, vector_column, rows) "
									   "VALUES ($1, $2::regclass, $3, $4, $5) ON CONFLICT (name) DO UPDATE "
									   "SET source = EXCLUDED.source, key_column = EXCLUDED.key_column, "
									   "vector_column = EXCLUDED.vector_column, rows = EXCLUDED.rows, "
									   "loaded_at = now()", CatalogName(fcinfo)),
							  5, types, values, NULL, false, 0) != SPI_OK_INSERT)
		elog(ERROR, "could not record ann index \"%s\"", name);
	SPI_finish();
}

static bool
LookupCatalog(FunctionCallInfo fcinfo, const char *name, AnnCatalogEntry *entry)
{
	Oid			types[1] = {TEXTOID};
	Datum		values[1];
	bool		found;

	values[0] = CStringGetTextDatum(name);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	if (SPI_execute_with_args(psprintf("SELECT source::text, key_column::text, vector_column::text "
									   "FROM %s WHERE name = $1", CatalogName(fcinfo)),
							  1, types, values, NULL, true, 1) != SPI_OK_SELECT)
		elog(ERROR, "could not read the ann index catalog");
	found = SPI_processed == 1;
	if (found)
	{
		MemoryContext old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

		entry->source = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
		entry->key = pstrdup(quote_identifier(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2)));
		entry->vector = pstrdup(quote_identifier(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3)));
		MemoryContextSwitchTo(old);
	}
	SPI_finish();
	return found;
}

static void
RequireSuperuser(const char *action)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to %s an ann index", action)));
}

/*
 * ann_index_load(name, path): load an index directory into the worker, or
 * into every node of ann.worker_address. Workers read the files, so the path
 * is resolved on each GPU node.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ann_index_load);
Datum
ann_index_load(PG_FUNCTION_ARGS)
{
	const char *name = CheckName(PG_GETARG_TEXT_PP(0));
	char	   *path = text_to_cstring(PG_GETARG_TEXT_PP(1));
	StringInfoData payload;
	uint32		status = 0;
	char	   *reply = NULL;
	uint32		replyLength;
	AnnIndexInfo info;
	bool		ok;

	RequireSuperuser("load");
	initStringInfo(&payload);
	Put32(&payload, strlen(name));
	appendBinaryStringInfo(&payload, name, strlen(name));
	appendBinaryStringInfo(&payload, path, strlen(path));
	if (AnnRemoteConfigured())
		ok = AnnRemoteBroadcast(ANN_OP_LOAD_INDEX, payload.data, payload.len, ANN_LOAD_TIMEOUT_MS,
								&status, &reply, &replyLength) > 0 || status != PG_UINT32_MAX;
	else
		ok = AnnWorkerCall(ANN_OP_LOAD_INDEX, payload.data, payload.len, ANN_LOAD_TIMEOUT_MS,
						   &status, &reply, &replyLength);
	CheckWorker(ok, status, name);
	ParseInfo(reply, &info);
	CheckSource(&info);
	RecordCatalog(fcinfo, name, &info);
	PG_RETURN_INT64((int64) info.rows);
}

/*
 * ann_index_attach(name): use an index another host already loaded on the
 * GPU nodes. Verifies this host's copy of the source table, then records it.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ann_index_attach);
Datum
ann_index_attach(PG_FUNCTION_ARGS)
{
	const char *name = CheckName(PG_GETARG_TEXT_PP(0));
	uint32		status = 0;
	char	   *reply = NULL;
	uint32		replyLength;
	AnnIndexInfo info;
	bool		ok;

	RequireSuperuser("attach");
	ok = IndexCall(ANN_OP_INDEX_INFO, name, strlen(name), 0, &status, &reply, &replyLength);
	CheckWorker(ok, status, name);
	ParseInfo(reply, &info);
	CheckSource(&info);
	RecordCatalog(fcinfo, name, &info);
	PG_RETURN_INT64((int64) info.rows);
}

static int
KeyCompare(const void *a, const void *b)
{
	int64		x = *(const int64 *) a;
	int64		y = *(const int64 *) b;

	return x < y ? -1 : x > y;
}

/* Sort when needed and drop duplicates; returns the new count. */
static int
SortKeys(int64 *keys, int nkeys)
{
	int			n = 0;

	for (int i = 1; i < nkeys; i++)
		if (keys[i] < keys[i - 1])
		{
			qsort(keys, nkeys, sizeof(int64), KeyCompare);
			break;
		}
	for (int i = 0; i < nkeys; i++)
		if (i == 0 || keys[i] != keys[i - 1])
			keys[n++] = keys[i];
	return n;
}

static void
PutVarint(StringInfo buffer, uint64 value)
{
	char		bytes[10];
	int			n = 0;

	do
	{
		bytes[n] = (char) (value & 0x7f);
		value >>= 7;
		if (value)
			bytes[n] |= (char) 0x80;
		n++;
	} while (value);
	appendBinaryStringInfo(buffer, bytes, n);
}

static void
InitResult(FunctionCallInfo fcinfo, TupleDesc *tupdesc, Tuplestorestate **store)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext oldcontext;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (get_call_result_type(fcinfo, NULL, tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	*tupdesc = CreateTupleDescCopy(*tupdesc);
	*store = tuplestore_begin_heap(true, false, work_mem);
	MemoryContextSwitchTo(oldcontext);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = *store;
	rsinfo->setDesc = *tupdesc;
}

/*
 * Ask a GPU node for the top k, restricted to sorted unique keys when
 * filtered. Keys travel as a zigzag varint first key and varint gaps.
 */
static void
SearchGpu(const char *name, Vector *query, int32 k, bool filtered, const int64 *keys, int nkeys,
		  TupleDesc tupdesc, Tuplestorestate *store)
{
	StringInfoData payload;
	StringInfoData packed;
	uint32		status = 0;
	char	   *reply = NULL;
	uint32		replyLength;
	uint32		count;
	bool		ok;

	initStringInfo(&packed);
	for (int i = 0; i < nkeys; i++)
	{
		if (i == 0)
			PutVarint(&packed, ((uint64) keys[0] << 1) ^ (uint64) (keys[0] >> 63));
		else
			PutVarint(&packed, (uint64) keys[i] - (uint64) keys[i - 1]);
	}
	initStringInfo(&payload);
	Put32(&payload, strlen(name));
	appendBinaryStringInfo(&payload, name, strlen(name));
	Put32(&payload, query->dim);
	Put32(&payload, k);
	Put32(&payload, filtered ? (uint32) nkeys : PG_UINT32_MAX);
	Put32(&payload, packed.len);
	appendBinaryStringInfo(&payload, (char *) query->x, sizeof(float) * query->dim);
	appendBinaryStringInfo(&payload, packed.data, packed.len);
	ok = IndexCall(ANN_OP_INDEX_SEARCH_PACKED, payload.data, payload.len, 0, &status, &reply, &replyLength);
	CheckWorker(ok, status, name);
	if (replyLength < 8 || (count = *(uint32 *) reply) != (replyLength - 8) / 12 ||
		(Size) count * 12 + 8 != replyLength)
		elog(ERROR, "malformed ann worker search reply");
	for (uint32 i = 0; i < count; i++)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};
		int64		key;
		float		distance;

		memcpy(&key, reply + 8 + (Size) i * 12, sizeof(key));
		memcpy(&distance, reply + 16 + (Size) i * 12, sizeof(distance));
		values[0] = Int64GetDatum(key);
		values[1] = Float4GetDatum(distance);
		tuplestore_putvalues(store, tupdesc, values, nulls);
	}
}

/*
 * Exact top k on this host with the SQL operator, over either eligible keys
 * or a filter predicate. The query plan is PostgreSQL's own.
 */
static void
SearchLocal(FunctionCallInfo fcinfo, const AnnCatalogEntry *entry, Datum query, int32 k,
			const char *filter, const int64 *keys, int nkeys, TupleDesc tupdesc, Tuplestorestate *store)
{
	Oid			types[3];
	Datum		values[3];
	char	   *sql;
	SPIPlanPtr	plan;

	types[0] = get_fn_expr_argtype(fcinfo->flinfo, 1);
	values[0] = query;
	types[1] = INT4OID;
	values[1] = Int32GetDatum(k);
	if (filter)
		sql = psprintf("SELECT %s::bigint, (%s <-> $1)::real FROM %s WHERE (%s) ORDER BY 2, 1 LIMIT $2",
					   entry->key, entry->vector, entry->source, filter);
	else
	{
		Datum	   *elements = palloc(sizeof(Datum) * Max(nkeys, 1));

		for (int i = 0; i < nkeys; i++)
			elements[i] = Int64GetDatum(keys[i]);
		types[2] = INT8ARRAYOID;
		values[2] = PointerGetDatum(construct_array(elements, nkeys, INT8OID, 8, FLOAT8PASSBYVAL, TYPALIGN_DOUBLE));
		sql = psprintf("SELECT %s::bigint, (%s <-> $1)::real FROM %s WHERE %s = ANY ($3) ORDER BY 2, 1 LIMIT $2",
					   entry->key, entry->vector, entry->source, entry->key);
	}
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* One prepared read-only statement: a filter cannot smuggle in writes. */
	plan = SPI_prepare(sql, filter ? 2 : 3, types);
	if (plan == NULL)
		elog(ERROR, "could not prepare ann local search: %s", SPI_result_code_string(SPI_result));
	if (SPI_execute_plan(plan, values, NULL, true, 0) != SPI_OK_SELECT)
		elog(ERROR, "ann local search failed");
	for (uint64 i = 0; i < SPI_processed; i++)
	{
		Datum		row[2];
		bool		nulls[2];

		row[0] = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &nulls[0]);
		row[1] = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &nulls[1]);
		tuplestore_putvalues(store, tupdesc, row, nulls);
	}
	SPI_finish();
}

static int32
CheckK(int32 k)
{
	if (k < 1 || k > ANN_INDEX_MAX_K)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("k must be between 1 and %d", ANN_INDEX_MAX_K)));
	return k;
}

/*
 * ann_index_search(name, query, k, filter): top-k (id, distance) from a
 * loaded index. A non-NULL filter restricts results to those keys, so any
 * SQL predicate can drive hybrid search; NULL elements are ignored. Filters
 * smaller than ann.gpu_min_eligible are scored exactly on this host when the
 * index is in the catalog.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ann_index_search);
Datum
ann_index_search(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Tuplestorestate *store;
	const char *name;
	int32		k;
	int64	   *keys = NULL;
	int			nkeys = 0;
	bool		filtered = !PG_ARGISNULL(3);
	AnnCatalogEntry entry;

	InitResult(fcinfo, &tupdesc, &store);
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		return (Datum) 0;
	name = CheckName(PG_GETARG_TEXT_PP(0));
	k = CheckK(PG_GETARG_INT32(2));
	if (filtered)
	{
		ArrayType  *array = PG_GETARG_ARRAYTYPE_P(3);
		Datum	   *values;
		bool	   *nulls;
		int			n;

		deconstruct_array(array, INT8OID, 8, FLOAT8PASSBYVAL, TYPALIGN_DOUBLE, &values, &nulls, &n);
		keys = palloc(sizeof(int64) * Max(n, 1));
		for (int i = 0; i < n; i++)
			if (!nulls[i])
				keys[nkeys++] = DatumGetInt64(values[i]);

		/* No eligible keys means no rows; skip the round trip. */
		if (nkeys == 0)
			return (Datum) 0;
		nkeys = SortKeys(keys, nkeys);
		if (nkeys < ann_gpu_min_eligible && LookupCatalog(fcinfo, name, &entry))
		{
			SearchLocal(fcinfo, &entry, PG_GETARG_DATUM(1), k, NULL, keys, nkeys, tupdesc, store);
			return (Datum) 0;
		}
	}
	SearchGpu(name, PG_GETARG_VECTOR_P(1), k, filtered, keys, nkeys, tupdesc, store);
	return (Datum) 0;
}

/*
 * ann_hybrid_search(name, query, k, filter): hybrid search where this host
 * evaluates the SQL filter on the index's source table. A capped count
 * decides the route: filters under ann.gpu_min_eligible rows run exactly
 * here; larger ones stream their keys to a GPU node. The filter is a SQL
 * boolean expression over the source table, run read-only with the
 * caller's privileges, like any dynamic SQL; quote values with format().
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ann_hybrid_search);
Datum
ann_hybrid_search(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Tuplestorestate *store;
	const char *name;
	int32		k;
	char	   *filter;
	AnnCatalogEntry entry;
	SPIPlanPtr	plan;
	Portal		portal;
	int64	   *keys;
	int			nkeys = 0;
	int			capacity = ANN_KEY_FETCH;
	int64		limit = (int64) ann_gpu_min_eligible;
	bool		isnull;
	int64		count;
	MemoryContext caller = CurrentMemoryContext;

	InitResult(fcinfo, &tupdesc, &store);
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		return (Datum) 0;
	name = CheckName(PG_GETARG_TEXT_PP(0));
	k = CheckK(PG_GETARG_INT32(2));
	filter = text_to_cstring(PG_GETARG_TEXT_PP(3));
	if (!LookupCatalog(fcinfo, name, &entry))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("ann index \"%s\" is not in this database's catalog", name),
				 errhint("Run ann_index_load or ann_index_attach on this host.")));
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	plan = SPI_prepare(psprintf("SELECT count(*) FROM (SELECT FROM %s WHERE (%s) LIMIT " INT64_FORMAT ") s",
								entry.source, filter, limit), 0, NULL);
	if (plan == NULL || SPI_execute_plan(plan, NULL, NULL, true, 1) != SPI_OK_SELECT)
		elog(ERROR, "could not evaluate the ann hybrid filter");
	count = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	if (count < limit)
	{
		SPI_finish();
		SearchLocal(fcinfo, &entry, PG_GETARG_DATUM(1), k, filter, NULL, 0, tupdesc, store);
		return (Datum) 0;
	}

	/* Broad filter: stream the eligible keys, then search on a GPU node. */
	keys = MemoryContextAlloc(caller, sizeof(int64) * capacity);
	plan = SPI_prepare(psprintf("SELECT %s::bigint FROM %s WHERE (%s)", entry.key, entry.source, filter), 0, NULL);
	if (plan == NULL || (portal = SPI_cursor_open(NULL, plan, NULL, NULL, true)) == NULL)
		elog(ERROR, "could not evaluate the ann hybrid filter");
	for (;;)
	{
		SPI_cursor_fetch(portal, true, ANN_KEY_FETCH);
		if (SPI_processed == 0)
			break;
		if (nkeys + SPI_processed > (uint64) capacity)
		{
			while (nkeys + SPI_processed > (uint64) capacity)
				capacity *= 2;
			keys = repalloc_huge(keys, sizeof(int64) * capacity);
		}
		for (uint64 i = 0; i < SPI_processed; i++)
		{
			Datum		value = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);

			if (!isnull)
				keys[nkeys++] = DatumGetInt64(value);
		}
		SPI_freetuptable(SPI_tuptable);
		CHECK_FOR_INTERRUPTS();
	}
	SPI_cursor_close(portal);
	SPI_finish();
	nkeys = SortKeys(keys, nkeys);
	SearchGpu(name, PG_GETARG_VECTOR_P(1), k, true, keys, nkeys, tupdesc, store);
	return (Datum) 0;
}
