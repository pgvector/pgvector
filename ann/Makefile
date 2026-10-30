# Standalone reference worker; independent of PGXS and GPU SDKs.
CC ?= cc
CFLAGS ?= -O2 -g
CFLAGS += -std=c11 -Wall -Wextra -Werror -pedantic
CUDA_HOME ?= /usr/local/cuda

all: ann-worker

ann-worker: worker.c cpu.c backend.h
	$(CC) $(CFLAGS) worker.c cpu.c -lm -o $@

# Requires the cuVS 26.10 C SDK (libcuvs_c), DLPack, and CUDA runtime headers.
# A different executable keeps the dependency-free CPU build reproducible.
ann-worker-cuvs: worker.c cpu.c cuvs.c backend.h
	@test -n "$(CUVS_PREFIX)" || { echo 'Set CUVS_PREFIX to the cuVS 26.10 installation' >&2; exit 1; }
	$(CC) $(CFLAGS) -DANN_USE_CUVS -I"$(CUVS_PREFIX)/include" -I"$(CUDA_HOME)/include" $(CPPFLAGS) worker.c cpu.c cuvs.c -L"$(CUVS_PREFIX)/lib" -L"$(CUDA_HOME)/lib64" $(LDFLAGS) -lcuvs_c -lcudart -lm -o $@

check: ann-worker
	python3 ../test/ann/test_worker.py
	python3 ../test/ann/test_cuvs.py
	python3 ../test/ann/test_postgres.py

check-cuvs: ann-worker ann-worker-cuvs
	ANN_CUVS_TEST=1 python3 ../test/ann/test_cuvs.py

check-postgres: ann-worker
	@test -n "$$ANN_PG_DSN" || { echo 'Set ANN_PG_DSN to a test database with pgvector installed' >&2; exit 1; }
	python3 ../test/ann/test_postgres.py

clean:
	rm -f ann-worker
	rm -f ann-worker-cuvs
	rm -rf ann-worker.dSYM
	rm -rf ann-worker-cuvs.dSYM

.PHONY: all check check-cuvs check-postgres clean
