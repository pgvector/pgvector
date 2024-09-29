use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my @dims = (384, 1536);
my $limit = 10;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

$node->safe_psql("postgres", "CREATE EXTENSION vector;");

for my $dim (@dims)
{
	my $array_sql = join(",", ('random()') x $dim);

	# Create table and index
	$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
	$node->safe_psql("postgres",
		"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(1, 2000) i;"
	);
	$node->safe_psql("postgres", "CREATE INDEX idx ON tst USING hnsw (v vector_l2_ops);");
	$node->safe_psql("postgres", "ANALYZE tst;");

	# Generate query
	my @r = ();
	for (1 .. $dim)
	{
		push(@r, rand());
	}
	my $query = "[" . join(",", @r) . "]";

	my $explain = $node->safe_psql("postgres", qq(
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v <-> '$query' LIMIT $limit;
	));
	like($explain, qr/Index Scan using idx/);

	# 3x the rows are needed for distance filters
	# since the planner uses DEFAULT_INEQ_SEL for the selectivity (should be 1)
	# Recreate index for performance
	$node->safe_psql("postgres", "DROP INDEX idx;");
	$node->safe_psql("postgres",
		"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(2001, 6000) i;"
	);
	$node->safe_psql("postgres", "CREATE INDEX idx ON tst USING hnsw (v vector_l2_ops);");
	$node->safe_psql("postgres", "ANALYZE tst;");

	$explain = $node->safe_psql("postgres", qq(
		EXPLAIN ANALYZE SELECT i FROM tst WHERE v <-> '$query' < 1 ORDER BY v <-> '$query' LIMIT $limit;
	));
	like($explain, qr/Index Scan using idx/);

	$node->safe_psql("postgres", "DROP TABLE tst;");
}

done_testing();
