#ifndef BITUTILS_H
#define BITUTILS_H

/* We use two types of dispatching: intrinsics and target_clones */
/* TODO Move to better place */
#ifndef DISABLE_DISPATCH
/* Only enable for more recent compilers to keep build process simple */
#if defined(__x86_64__) && defined(__GNUC__) && __GNUC__ >= 8
#define USE_DISPATCH
#elif defined(__x86_64__) && defined(__clang_major__) && __clang_major__ >= 7
#define USE_DISPATCH
#elif defined(_M_AMD64) && defined(_MSC_VER) && _MSC_VER >= 1920
#define USE_DISPATCH
#endif
#endif

/* target_clones requires glibc */
#if defined(USE_DISPATCH) && defined(__gnu_linux__)
#define USE_TARGET_CLONES
#endif

#if defined(USE_DISPATCH)
#define BIT_DISPATCH
#endif

extern uint64 (*BitHammingDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance);
extern double (*BitJaccardDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa, uint64 bb);

void		BitvecInit(void);

#endif
