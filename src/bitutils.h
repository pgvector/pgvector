#ifndef BITUTILS_H
#define BITUTILS_H

#include "postgres.h"

/* Check version in first header */
#if PG_VERSION_NUM < 130000
#error "Requires PostgreSQL 13+"
#endif

extern uint64 (*BitHammingDistance) (uint32 bytes, const unsigned char *ax, const unsigned char *bx, uint64 distance);
extern double (*BitJaccardDistance) (uint32 bytes, const unsigned char *ax, const unsigned char *bx, uint64 ab, uint64 aa, uint64 bb);

void		BitvecInit(void);

#endif
