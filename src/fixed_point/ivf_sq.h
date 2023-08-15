#ifndef PGVECTOR_SRC_FIXED_POINT_IVF_SQ_H_
#define PGVECTOR_SRC_FIXED_POINT_IVF_SQ_H_

#include "../ivfflat.h"
#include "../vector.h"
#include "common/relpath.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"

#define IVFSQ_MAX_DIM 8000

typedef struct IvfsqMetaPageData {
  /* Fields inherited from `ivfflat`. */
  IvfflatMetaPageData base;
  /* Location of multipliers, always stored on external pages. */
  ItemPointerData multipliers_loc;
} IvfsqMetaPageData;

typedef IvfsqMetaPageData* IvfsqMetaPage;

#define IvfsqPageGetMeta(page) ((IvfsqMetaPageData*)PageGetContents(page))

/*
 * This function writes the multipliers into the IVF meta page for `index`. The
 * multipliers are *always* written as `ExternalMetadata` (i.e., only the
 * location gets written into the meta page).
 */ 
void UpdateMetaPageWithMultipliers(Relation index, const Vector* multipliers,
                                   ForkNumber fork_num);

/*
 * This function reads the multipliers based on the location specified
 * in meta page in `index`. On success, it allocates the memory for
 * `multipliers` and the caller is expected to release it.
 */ 
Vector* IvfsqGetMultipliers(Relation index, ForkNumber fork_num);

#endif  /* PGVECTOR_SRC_FIXED_POINT_IVF_SQ_H_ */
