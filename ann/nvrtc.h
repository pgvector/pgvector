#ifndef ANN_NVRTC_H
#define ANN_NVRTC_H

#include <stddef.h>

/*
 * Compile CUDA C++ source to a CUBIN for one GPU architecture (for example
 * "sm_100a") with NVRTC, opened at run time so building the worker needs no
 * CUDA compiler. Returns the image (free it) and its size, or NULL after
 * printing NVRTC's log under the given label.
 */
char	   *AnnNvrtcCompile(const char *source, const char *label, const char *arch, size_t *size);

#endif
