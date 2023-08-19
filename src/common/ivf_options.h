#ifndef PGVECTOR_SRC_COMMON_IVF_OPTIONS_H_
#define PGVECTOR_SRC_COMMON_IVF_OPTIONS_H_

#include <stdint.h>

#include "c.h"
#include "postgres.h"
#include "utils/relcache.h"

extern int ivf_probes;

typedef enum IvfQuantizerType
{
	kIvfflat = 0,
	kIvfsq8 = 1,
	kIvfInvalid = 2,
} IvfQuantizerType;

/* IVF index options */
typedef struct IvfOptions
{
	int32_t vl_len_; /* varlena header (do not touch directly!) */
	int lists;		 /* number of lists */
	int quantizer;
} IvfOptions;

/* IVF Option Accessors
 * --------------------
 * These APIs can be called for both `ivfflat` and `ivf` indexes.
 *
 * Get the number of lists configured for `index`.
 *
 * RETURNS:
 *   Number of lists, or -1 on error.
 */
int IvfGetLists(Relation index);

/* Get the quantization method configured for `index`.
 *
 * RETURNS:
 *   The quantization type, or `kIvfInvalid` on error.
 */
IvfQuantizerType IvfGetQuantizer(Relation index);

/*
 * Initialize ivf index options.
 */
void IvfInit(void);

/*
 * Parses and validates the relation options for ivf index type.
 */
bytea *ivfoptions(Datum reloptions, bool validate);

#endif /* PGVECTOR_SRC_COMMON_IVF_OPTIONS_H_ */
