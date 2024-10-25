use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my @queries = ();
my @expected;
my $limit = 20;
my $dim = 3;
my $array_sql = join(",", ('random()') x $dim);
my @cs = (50, 500);

sub test_recall
{
	my ($c, $ef_search, $min, $operator, $mode) = @_;
	my $correct = 0;
	my $total = 0;

	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET hnsw.ef_search = $ef_search;
		SET hnsw.iterative_scan = $mode;
		EXPLAIN ANALYZE SELECT i FROM tst WHERE i % $c = 0 ORDER BY v $operator '$queries[0]' LIMIT $limit;
	));
	like($explain, qr/Index Scan using idx on tst/);

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET hnsw.ef_search = $ef_search;
			SET hnsw.iterative_scan = $mode;
			SELECT i FROM tst WHERE i % $c = 0 ORDER BY v $operator '$queries[$i]' LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);

		my @expected_ids = split("\n", $expected[$i]);
		my %expected_set = map { $_ => 1 } @expected_ids;

		foreach (@actual_ids)
		{
			if (exists($expected_set{$_}))
			{
				$correct++;
			}
		}

		$total += $limit;
	}

	cmp_ok($correct / $total, ">=", $min, "$operator $mode $c");
}

# Initialize node
$node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(1, 50000) i;"
);

# Generate queries
for (1 .. 20)
{
	my @r = ();
	for (1 .. $dim)
	{
		push(@r, rand());
	}
	push(@queries, "[" . join(",", @r) . "]");
}

# Check each index type
my @operators = ("<->", "<=>");
my @opclasses = ("vector_l2_ops", "vector_cosine_ops");

for my $i (0 .. $#operators)
{
	my $operator = $operators[$i];
	my $opclass = $opclasses[$i];

	$node->safe_psql("postgres", qq(
		SET maintenance_work_mem = '128MB';
		CREATE INDEX idx ON tst USING hnsw (v $opclass);
	));

	foreach (@cs)
	{
		my $c = $_;

		# Get exact results
		@expected = ();
		foreach (@queries)
		{
			my $res = $node->safe_psql("postgres", qq(
				SET enable_indexscan = off;
				WITH top AS (
					SELECT v $operator '$_' AS distance FROM tst WHERE i % $c = 0 ORDER BY distance LIMIT $limit
				)
				SELECT i FROM tst WHERE (v $operator '$_') <= (SELECT MAX(distance) FROM top)
			));
			push(@expected, $res);
		}

		test_recall($c, 40, 0.99, $operator, "strict_order");
		test_recall($c, 40, 0.99, $operator, "relaxed_order");
	}

	$node->safe_psql("postgres", "DROP INDEX idx;");
}

done_testing();
