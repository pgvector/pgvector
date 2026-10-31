# Standalone reference worker; independent of PGXS and GPU SDKs.
CC ?= cc
CFLAGS ?= -O2 -g
CFLAGS += -std=c11 -Wall -Wextra -Werror -pedantic
CUDA_HOME ?= /usr/local/cuda

# USE_CUVS=1 also builds the cuVS worker (requires CUVS_PREFIX); hosts
# without a GPU SDK keep the default CPU-only build.
USE_CUVS ?= 0

ifeq ($(USE_CUVS),1)
all: ann-worker libannbatch.a ann-worker-cuvs
else
all: ann-worker libannbatch.a
endif

batching.o: batching.c batching.h backend.h
	$(CC) $(CFLAGS) -pthread -c batching.c -o $@

cpu.o: cpu.c backend.h indexfile.h
	$(CC) $(CFLAGS) -c cpu.c -o $@

indexfile.o: indexfile.c indexfile.h backend.h
	$(CC) $(CFLAGS) -pthread -c indexfile.c -o $@

libannbatch.a: batching.o cpu.o indexfile.o
	$(AR) rcs $@ batching.o cpu.o indexfile.o

ann-batch-tests: ../test/ann/batch_test.c batching.c cpu.c indexfile.c batching.h backend.h
	$(CC) $(CFLAGS) -pthread -I. ../test/ann/batch_test.c batching.c cpu.c indexfile.c -lm -o $@

ann-batch-bench: ../benchmarks/ann/batch.c batching.c cpu.c indexfile.c batching.h backend.h
	$(CC) $(CFLAGS) -pthread -I. ../benchmarks/ann/batch.c batching.c cpu.c indexfile.c -lm -o $@

WORKER_SRCS = worker.c cpu.c mockgpu.c batching.c server.c indexfile.c
WORKER_DEPS = $(WORKER_SRCS) batching.h backend.h server.h indexfile.h

ann-worker: $(WORKER_DEPS)
	$(CC) $(CFLAGS) -pthread $(WORKER_SRCS) -lm -o $@

# Requires the cuVS 26.10 C SDK (libcuvs_c), DLPack, and CUDA runtime headers.
# A different executable keeps the dependency-free CPU build reproducible.
ann-worker-cuvs: $(WORKER_DEPS) cuvs.c
	@test -n "$(CUVS_PREFIX)" || { echo 'Set CUVS_PREFIX to the cuVS 26.10 installation' >&2; exit 1; }
	$(CC) $(CFLAGS) -pthread -DANN_USE_CUVS -I"$(CUVS_PREFIX)/include" -I"$(CUDA_HOME)/include" $(CPPFLAGS) $(WORKER_SRCS) cuvs.c -L"$(CUVS_PREFIX)/lib" -L"$(CUVS_PREFIX)/lib64" -L"$(CUDA_HOME)/lib64" $(LDFLAGS) -lcuvs_c -lcudart -lm -o $@

check: ann-worker ann-batch-tests
	python3 ../test/ann/test_worker.py
	python3 ../test/ann/test_batching.py
	python3 ../test/ann/test_server.py
	python3 ../test/ann/test_index.py
	python3 ../test/ann/test_cuvs.py
	python3 ../test/ann/test_postgres.py
	python3 ../test/ann/test_native.py
	python3 ../test/ann/test_filtered.py
	python3 ../test/ann/test_pipeline.py

check-cuvs: ann-worker ann-worker-cuvs
	ANN_CUVS_TEST=1 python3 ../test/ann/test_cuvs.py
	ANN_CUVS_TEST=1 python3 ../test/ann/test_cuvs_gathered.py
	ANN_CUVS_TEST=1 python3 ../test/ann/test_cuvs_index.py

check-postgres: ann-worker
	@test -n "$$ANN_PG_DSN" || { echo 'Set ANN_PG_DSN to a test database with pgvector installed' >&2; exit 1; }
	python3 ../test/ann/test_postgres.py
	python3 ../test/ann/test_native.py
	python3 ../test/ann/test_filtered.py
	python3 ../test/ann/test_pipeline.py
	python3 ../test/ann/test_index.py

clean:
	rm -f ann-worker
	rm -f batching.o cpu.o indexfile.o libannbatch.a ann-batch-tests
	rm -f ann-batch-bench
	rm -rf ann-batch-bench.dSYM
	rm -rf ann-batch-tests.dSYM
	rm -f ann-worker-cuvs
	rm -rf ann-worker.dSYM
	rm -rf ann-worker-cuvs.dSYM

.PHONY: all check check-cuvs check-postgres clean
