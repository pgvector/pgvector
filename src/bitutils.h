#ifndef BITUTILS_H
#define BITUTILS_H

#include "postgres.h"

/* Check version in first header */
#if PG_VERSION_NUM < 120000
#error "Requires PostgreSQL 12+"
#endif

extern uint64 (*BitHammingDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance);
extern double (*BitJaccardDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa, uint64 bb);

void		BitvecInit(void);

#endif
