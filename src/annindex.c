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
#define ANN_OP_INDEX_SEARCH 12
#define ANN_INDEX_NAME_MAX 63
#define ANN_INDEX_MAX_K 10000
#define ANN_LOAD_TIMEOUT_MS (30 * 60 * 1000)

/* Statuses shared with ann/backend.h. */
#define ANN_STATUS_OK 0
#define ANN_STATUS_INVALID 1
#define ANN_STATUS_UNSUPPORTED 2
#define ANN_STATUS_STALE 3
#define ANN_STATUS_OVERLOADED 7

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

static void
CheckWorker(bool ok, uint32 status, const char *name)
{
	if (!ok)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("ann worker is unavailable"),
				 errhint("Set ann.worker_socket to a running ann-worker --listen socket.")));
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

/*
 * The index is immutable: refuse to serve one whose source table no longer
 * has the row count it was built from. Later writes are the operator's
 * contract; rebuild the index after changing the table.
 */
static void
CheckSource(const char *source, uint64 rows)
{
	StringInfoData query;
	List	   *names;
	bool		isnull;
	int64		count;

	if (strcmp(source, "-") == 0)
		return;
	names = stringToQualifiedNameList(source
#if PG_VERSION_NUM >= 160000
									  ,NULL
#endif
		);
	initStringInfo(&query);
	appendStringInfo(&query, "SELECT count(*) FROM %s", NameListToQuotedString(names));
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	if (SPI_execute(query.data, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "could not count rows of %s", source);
	count = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	SPI_finish();
	if (count != (int64) rows)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("ann index is stale: %s has " INT64_FORMAT " rows, index has " UINT64_FORMAT,
						source, count, rows),
				 errhint("Rebuild the index with scripts/ann/build_cuvs_index.py.")));
}

/*
 * ann_index_load(name, path): load an index directory into the worker. The
 * worker reads the files, so the path is resolved on the worker's host.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ann_index_load);
Datum
ann_index_load(PG_FUNCTION_ARGS)
{
	const char *name = CheckName(PG_GETARG_TEXT_PP(0));
	char	   *path = text_to_cstring(PG_GETARG_TEXT_PP(1));
	StringInfoData payload;
	uint32		status;
	char	   *reply;
	uint32		replyLength;
	unsigned long long rows;
	char		source[256];
	bool		ok;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to load an ann index")));
	initStringInfo(&payload);
	Put32(&payload, strlen(name));
	appendBinaryStringInfo(&payload, name, strlen(name));
	appendBinaryStringInfo(&payload, path, strlen(path));
	ok = AnnWorkerCall(ANN_OP_LOAD_INDEX, payload.data, payload.len, ANN_LOAD_TIMEOUT_MS,
					   &status, &reply, &replyLength);
	CheckWorker(ok, status, name);
	if (sscanf(reply, "rows %llu dimensions %*u shards %*u dtype %*s source %255s", &rows, source) != 2)
		elog(ERROR, "unexpected ann worker load reply: %s", reply);
	CheckSource(source, rows);
	PG_RETURN_INT64((int64) rows);
}

static int
KeyCompare(const void *a, const void *b)
{
	int64		x = *(const int64 *) a;
	int64		y = *(const int64 *) b;

	return x < y ? -1 : x > y;
}

/*
 * ann_index_search(name, query, k, filter): top-k (id, distance) from a
 * loaded index. A non-NULL filter restricts results to those keys, so any
 * SQL predicate can drive hybrid search; NULL elements are ignored.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ann_index_search);
Datum
ann_index_search(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *store;
	MemoryContext oldcontext;
	const char *name;
	Vector	   *query;
	int32		k;
	StringInfoData payload;
	int64	   *keys = NULL;
	int			nkeys = 0;
	bool		filtered = !PG_ARGISNULL(3);
	uint32		status;
	char	   *reply;
	uint32		replyLength;
	uint32		count;
	bool		ok;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ann_index_search must be called in a context that accepts a set")));
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupdesc = CreateTupleDescCopy(tupdesc);
	store = tuplestore_begin_heap(true, false, work_mem);
	MemoryContextSwitchTo(oldcontext);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = store;
	rsinfo->setDesc = tupdesc;
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		return (Datum) 0;
	name = CheckName(PG_GETARG_TEXT_PP(0));
	query = PG_GETARG_VECTOR_P(1);
	k = PG_GETARG_INT32(2);
	if (k < 1 || k > ANN_INDEX_MAX_K)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("k must be between 1 and %d", ANN_INDEX_MAX_K)));
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

		/* Index-only scans usually produce sorted keys already. */
		for (int i = 1; i < nkeys; i++)
			if (keys[i] < keys[i - 1])
			{
				qsort(keys, nkeys, sizeof(int64), KeyCompare);
				break;
			}
	}
	initStringInfo(&payload);
	Put32(&payload, strlen(name));
	appendBinaryStringInfo(&payload, name, strlen(name));
	Put32(&payload, query->dim);
	Put32(&payload, k);
	Put32(&payload, filtered ? (uint32) nkeys : PG_UINT32_MAX);
	appendBinaryStringInfo(&payload, (char *) query->x, sizeof(float) * query->dim);
	if (nkeys)
		appendBinaryStringInfo(&payload, (char *) keys, sizeof(int64) * nkeys);
	ok = AnnWorkerCall(ANN_OP_INDEX_SEARCH, payload.data, payload.len, 0, &status, &reply, &replyLength);
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
	return (Datum) 0;
}
