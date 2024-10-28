use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $dim = 3;
my $array_sql = join(",", ('random()') x $dim);

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4 PRIMARY KEY, v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(1, 100000) i;"
);
$node->safe_psql("postgres", qq(
	SET maintenance_work_mem = '128MB';
	SET max_parallel_maintenance_workers = 2;
	CREATE INDEX ON tst USING hnsw (v vector_l2_ops)
));

my $count = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	SET hnsw.iterative_scan = relaxed_order;
	SET hnsw.max_scan_tuples = 100000;
	SET hnsw.scan_mem_multiplier = 2;
	SELECT COUNT(*) FROM (SELECT v FROM tst WHERE i % 10000 = 0 ORDER BY v <-> (SELECT v FROM tst LIMIT 1) LIMIT 11) t;
));
is($count, 10);

foreach ((30000, 50000, 70000))
{
	my $max_tuples = $_;
	my $expected = $max_tuples / 10000;
	my $sum = 0;

	for my $i (1 .. 20)
	{
		$count = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET hnsw.iterative_scan = relaxed_order;
			SET hnsw.max_scan_tuples = $max_tuples;
			SET hnsw.scan_mem_multiplier = 2;
			SELECT COUNT(*) FROM (SELECT v FROM tst WHERE i % 10000 = 0 ORDER BY v <-> (SELECT v FROM tst WHERE i = $i) LIMIT 11) t;
		));
		$sum += $count;
	}

	my $avg = $sum / 20;
	cmp_ok($avg, '>', $expected - 2);
	cmp_ok($avg, '<', $expected + 2);
}

done_testing();
