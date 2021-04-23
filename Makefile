EXTENSION = vector
DATA = vector--0.1.1.sql vector--0.1.0-0.1.1.sql
MODULE_big = vector
OBJS = src/ivfbuild.o src/ivfflat.o src/ivfinsert.o src/ivfkmeans.o src/ivfscan.o src/ivfutils.o src/ivfvacuum.o src/vector.o

TESTS = $(wildcard sql/*.sql)

REGRESS = btree cast copy functions ivfflat_cosine ivfflat_ip ivfflat_l2 ivfflat_unlogged vector

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

prove_installcheck:
	rm -rf $(CURDIR)/tmp_check
	cd $(srcdir) && TESTDIR='$(CURDIR)' PATH="$(bindir):$$PATH" PGPORT='6$(DEF_PGPORT)' PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' $(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),t/*.pl)
