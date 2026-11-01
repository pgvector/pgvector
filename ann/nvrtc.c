/*
 * NVRTC loader shared by the cuVS backend's gather kernels and the
 * Blackwell module. NVRTC is opened with dlopen, once.
 */
#include "nvrtc.h"

#include <dlfcn.h>
#include <nvrtc.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

static struct
{
	nvrtcResult (*create) (nvrtcProgram *, const char *, const char *, int, const char *const *,
						   const char *const *);
	nvrtcResult (*compile) (nvrtcProgram, int, const char *const *);
	nvrtcResult (*cubinSize) (nvrtcProgram, size_t *);
	nvrtcResult (*cubin) (nvrtcProgram, char *);
	nvrtcResult (*logSize) (nvrtcProgram, size_t *);
	nvrtcResult (*log) (nvrtcProgram, char *);
	nvrtcResult (*destroy) (nvrtcProgram *);
}			nvrtc;

static pthread_mutex_t nvrtcMutex = PTHREAD_MUTEX_INITIALIZER;

static int
OpenNvrtc(void)
{
	static const char *names[] = {"libnvrtc.so", "libnvrtc.so.13", "libnvrtc.so.12"};
	void	   *library = NULL;

	if (nvrtc.destroy)
		return 1;
	for (size_t i = 0; library == NULL && i < sizeof(names) / sizeof(names[0]); i++)
		library = dlopen(names[i], RTLD_NOW | RTLD_LOCAL);
	if (library == NULL)
		return 0;
	/* Assigning through void ** keeps ISO C object/function casts out. */
	*(void **) &nvrtc.create = dlsym(library, "nvrtcCreateProgram");
	*(void **) &nvrtc.compile = dlsym(library, "nvrtcCompileProgram");
	*(void **) &nvrtc.cubinSize = dlsym(library, "nvrtcGetCUBINSize");
	*(void **) &nvrtc.cubin = dlsym(library, "nvrtcGetCUBIN");
	*(void **) &nvrtc.logSize = dlsym(library, "nvrtcGetProgramLogSize");
	*(void **) &nvrtc.log = dlsym(library, "nvrtcGetProgramLog");
	*(void **) &nvrtc.destroy = dlsym(library, "nvrtcDestroyProgram");
	if (!nvrtc.create || !nvrtc.compile || !nvrtc.cubinSize || !nvrtc.cubin || !nvrtc.logSize ||
		!nvrtc.log || !nvrtc.destroy)
	{
		nvrtc.destroy = NULL;
		return 0;
	}
	return 1;
}

char *
AnnNvrtcCompile(const char *source, const char *label, const char *arch, size_t *size)
{
	nvrtcProgram program = NULL;
	char		option[64];
	char	   *image = NULL;
	int			ok;

	snprintf(option, sizeof(option), "--gpu-architecture=%s", arch);
	(void) pthread_mutex_lock(&nvrtcMutex);
	ok = OpenNvrtc();
	(void) pthread_mutex_unlock(&nvrtcMutex);
	if (!ok)
		return NULL;
	{
		const char *options[] = {option, "--std=c++17", "-default-device"};

		ok = nvrtc.create(&program, source, label, 0, NULL, NULL) == NVRTC_SUCCESS &&
			nvrtc.compile(program, 3, options) == NVRTC_SUCCESS &&
			nvrtc.cubinSize(program, size) == NVRTC_SUCCESS && *size > 0 &&
			(image = malloc(*size)) != NULL && nvrtc.cubin(program, image) == NVRTC_SUCCESS;
	}
	if (!ok && program)
	{
		size_t		logSize = 0;
		char	   *log;

		if (nvrtc.logSize(program, &logSize) == NVRTC_SUCCESS && logSize > 1 &&
			(log = malloc(logSize)) != NULL)
		{
			if (nvrtc.log(program, log) == NVRTC_SUCCESS)
				fprintf(stderr, "%s (%s): %s\n", label, arch, log);
			free(log);
		}
	}
	if (program)
		(void) nvrtc.destroy(&program);
	if (!ok)
	{
		free(image);
		return NULL;
	}
	return image;
}
