#ifndef BITUTILS_H
#define BITUTILS_H

/* Only enable for more recent compilers */
#if defined(__x86_64__) && defined(__GNUC__) && __GNUC__ >= 8
#define BIT_DISPATCH
#elif defined(__x86_64__) && defined(__clang_major__) && __clang_major__ >= 7
#define BIT_DISPATCH
#elif defined(_M_AMD64) && defined(_MSC_VER) && _MSC_VER >= 1920
#define BIT_DISPATCH
#endif

/* target_clones requires glibc */
#if defined(BIT_DISPATCH) && defined(__gnu_linux__)
#define USE_TARGET_CLONES
#endif

extern uint64 (*BitHammingDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance);
extern double (*BitJaccardDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa, uint64 bb);

void		BitvecInit(void);

#endif
