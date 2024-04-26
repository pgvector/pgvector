#ifndef BITUTILS_H
#define BITUTILS_H

#include "postgres.h"

/* Check version in first header */
#if PG_VERSION_NUM < 120000
#error "Requires PostgreSQL 12+"
#endif

#include "halfvec.h"			/* for USE_DISPATCH and USE_TARGET_CLONES */

#if defined(USE_DISPATCH)
#define BIT_DISPATCH
#endif

extern uint64 (*BitHammingDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance);
extern double (*BitJaccardDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa, uint64 bb);

void		BitvecInit(void);

#endif
