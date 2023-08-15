#include <stddef.h>
#include "ivf_options.h"

#include "../ivfflat.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "c.h"
#include "catalog/pg_am.h"
#include "postgres.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#define IVFFLAT_AM_NAME "ivfflat"
#define IVF_AM_NAME "ivf"

#define IVF_DEFAULT_QUANTIZER kIvfsq8

/* Custom relation option kind for IVF. */
static relopt_kind ivf_relopt_kind;
/* The quantization method. */
int ivf_quantizer;
/* The number of lists to probe during scan. */
int ivf_probes;

relopt_enum_elt_def IvfQuantizerTypeEnumValues[] = {
	{"SQ8", kIvfsq8},
	{(const char *)NULL} /* list terminator */
};

int IvfGetLists(Relation index)
{
	HeapTuple tuple;
	Form_pg_am amform;
	int lists = -1;

	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(index->rd_rel->relam));
	if (!HeapTupleIsValid(tuple))
		return -1;

	amform = (Form_pg_am)GETSTRUCT(tuple);
	if (strcmp(NameStr(amform->amname), IVFFLAT_AM_NAME) == 0)
		lists = IvfflatGetLists(index);
	else if (strcmp(NameStr(amform->amname), IVF_AM_NAME) == 0)
	{
		IvfOptions *opts = (IvfOptions *)index->rd_options;
		lists = (opts != NULL) ? opts->lists : IVFFLAT_DEFAULT_LISTS;
	}
	else
		elog(ERROR, "Unknown index AM: %s", NameStr(amform->amname));

	ReleaseSysCache(tuple);
	return lists;
}

IvfQuantizerType IvfGetQuantizer(Relation index)
{
	HeapTuple tuple;
	Form_pg_am amform;
	IvfQuantizerType quantizer = kIvfInvalid;

	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(index->rd_rel->relam));
	if (!HeapTupleIsValid(tuple))
		return kIvfInvalid;

	amform = (Form_pg_am)GETSTRUCT(tuple);
	if (strcmp(NameStr(amform->amname), IVFFLAT_AM_NAME) == 0)
		quantizer = kIvfflat;
	else if (strcmp(NameStr(amform->amname), IVF_AM_NAME) == 0)
	{
		IvfOptions *opts = (IvfOptions *)index->rd_options;
		quantizer = (opts != NULL) ? opts->quantizer : IVF_DEFAULT_QUANTIZER;
	}
	else
		elog(ERROR, "Unknown index AM: %s", NameStr(amform->amname));

	ReleaseSysCache(tuple);
	return quantizer;
}

void IvfInit()
{
	ivf_relopt_kind = add_reloption_kind();

	add_int_reloption(ivf_relopt_kind, "lists", "Number of inverted lists",
					  IVFFLAT_DEFAULT_LISTS, 1, IVFFLAT_MAX_LISTS
#if PG_VERSION_NUM >= 130000
					  ,
					  AccessExclusiveLock
#endif
	);
	add_enum_reloption(ivf_relopt_kind, "quantizer",
					   "Quantization option for ivf index",
					   IvfQuantizerTypeEnumValues, IVF_DEFAULT_QUANTIZER,
					   "The valid option for quantization is \"SQ8\"."
#if PG_VERSION_NUM >= 130000
					   ,
					   AccessExclusiveLock
#endif
	);
	DefineCustomIntVariable("ivf.probes", "Sets the number of probes",
							"Valid range is 1..lists.", &ivf_probes, 1, 1,
							IVFFLAT_MAX_LISTS, PGC_USERSET, 0, NULL, NULL, NULL);
}

bytea *ivfoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"lists", RELOPT_TYPE_INT, offsetof(IvfOptions, lists)},
		{"quantizer", RELOPT_TYPE_ENUM, offsetof(IvfOptions, quantizer)},
	};

#if PG_VERSION_NUM >= 130000
	return (bytea *)build_reloptions(reloptions, validate, ivf_relopt_kind,
									 sizeof(IvfOptions), tab, lengthof(tab));
#else
	relopt_value *options;
	int numoptions;
	IvfOptions *rdopts;

	options = parseRelOptions(reloptions, validate, ivf_relopt_kind, &numoptions);
	rdopts = allocateReloptStruct(sizeof(IvfOptions), options, numoptions);
	fillRelOptions((void *)rdopts, sizeof(IvfOptions), options, numoptions,
				   validate, tab, lengthof(tab));

	return (bytea *)rdopts;
#endif
}
