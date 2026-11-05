use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# IVFFlat scans must return rows in non-decreasing distance order and, when
# all lists are probed, exactly the rows of a sequential scan.
#
# Items are kept in memory while they fit in half of work_mem (4MB below) and
# are sorted with a tuplesort otherwise (64kB below), so cover both, as well as
# iterative scans with a filter, rescans (nested loop joins), and duplicate
# distances (ties).

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;
$node->safe_psql("postgres", "CREATE EXTENSION vector;");

# Integer coordinates so there are many duplicate distances
$node->safe_psql("postgres", qq(
	SELECT setseed(0.5);
	CREATE TABLE tst (i int4, c int4, v vector(8));
	INSERT INTO tst SELECT i, i % 10, ARRAY(SELECT floor(random() * 8) FROM generate_series(1, 8) WHERE i > 0) FROM generate_series(1, 6000) i;
	INSERT INTO tst SELECT 6000 + i, i % 10, v FROM tst WHERE i <= 500;
	CREATE INDEX ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 10);
	ANALYZE tst;
));

my @queries = ("[0,0,0,0,0,0,0,0]", "[3,3,3,3,3,3,3,3]", "[7,1,6,2,5,3,4,0]");

for my $wm ("64kB", "4MB")
{
	for my $q (@queries)
	{
		# All lists probed: same ordered distances as an exact scan
		my $exact = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off; SET work_mem = '$wm';
			SELECT string_agg((v <-> '$q')::text, ',' ORDER BY v <-> '$q') FROM tst;
		));
		my $ivf = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off; SET work_mem = '$wm'; SET ivfflat.probes = 10;
			SELECT string_agg(d::text, ',' ORDER BY n) FROM (SELECT row_number() OVER () AS n, d FROM (SELECT v <-> '$q' AS d FROM tst ORDER BY v <-> '$q') s) t;
		));
		is($ivf, $exact, "work_mem $wm query $q: all distances in order");

		# Every row is returned exactly once
		my $count = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off; SET work_mem = '$wm'; SET ivfflat.probes = 10;
			SELECT count(*), count(DISTINCT i) FROM (SELECT i FROM tst ORDER BY v <-> '$q') s;
		));
		is($count, "6500|6500", "work_mem $wm query $q: all rows returned once");

		# Top 10 with ties
		my $top_exact = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT string_agg(d::text, ',' ORDER BY d) FROM (SELECT v <-> '$q' AS d FROM tst ORDER BY d LIMIT 10) s;
		));
		my $top_ivf = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off; SET work_mem = '$wm'; SET ivfflat.probes = 10;
			SELECT string_agg(d::text, ',' ORDER BY d) FROM (SELECT v <-> '$q' AS d FROM tst ORDER BY v <-> '$q' LIMIT 10) s;
		));
		is($top_ivf, $top_exact, "work_mem $wm query $q: top 10 distances");

		# Iterative scan with a filter
		my $iter = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off; SET work_mem = '$wm'; SET ivfflat.probes = 1;
			SET ivfflat.iterative_scan = relaxed_order; SET ivfflat.max_probes = 10;
			SELECT count(*) FROM (SELECT i FROM tst WHERE c = 3 ORDER BY v <-> '$q' LIMIT 50) s;
		));
		is($iter, "50", "work_mem $wm query $q: iterative scan with filter");
	}

	# Rescans: every outer row restarts the index scan, which must start from
	# a clean state (including after a previous scan used the tuplesort)
	my $nested_exact = $node->safe_psql("postgres", qq(
		SET enable_indexscan = off; SET work_mem = '$wm';
		SELECT string_agg(d::text, ',' ORDER BY i, d) FROM (
			SELECT q.i, x.d FROM tst q CROSS JOIN LATERAL (SELECT t.v <-> q.v AS d FROM tst t ORDER BY t.v <-> q.v LIMIT 3 OFFSET 1) x WHERE q.i <= 100
		) s;
	));
	my $nested_ivf = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off; SET work_mem = '$wm'; SET ivfflat.probes = 10;
		SELECT string_agg(d::text, ',' ORDER BY i, d) FROM (
			SELECT q.i, x.d FROM tst q CROSS JOIN LATERAL (SELECT t.v <-> q.v AS d FROM tst t ORDER BY t.v <-> q.v LIMIT 3 OFFSET 1) x WHERE q.i <= 100
		) s;
	));
	is($nested_ivf, $nested_exact, "work_mem $wm: nested loop rescans");
}

done_testing();
