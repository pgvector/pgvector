use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my @queries = ();
my @expected;
my $limit = 20;

sub test_recall
{
	my ($probes, $min, $operator) = @_;
	my $correct = 0;
	my $total = 0;

	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET ivfflat.probes = $probes;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v $operator '$queries[0]' LIMIT $limit;
	));
	like($explain, qr/Index Scan using idx on tst/);

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET ivfflat.probes = $probes;
			SELECT i FROM tst ORDER BY v $operator '$queries[$i]' LIMIT $limit;
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

	cmp_ok($correct / $total, ">=", $min, $operator);
}

# Initialize node
$node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[random(), random(), random()] FROM generate_series(1, 100000) i;"
);

# Generate queries
for (1 .. 20)
{
	my $r1 = rand();
	my $r2 = rand();
	my $r3 = rand();
	push(@queries, "[$r1,$r2,$r3]");
}

# Check each index type
my @operators = ("<->", "<#>", "<=>");
my @opclasses = ("vector_l2_ops", "vector_ip_ops", "vector_cosine_ops");

for my $i (0 .. $#operators)
{
	my $operator = $operators[$i];
	my $opclass = $opclasses[$i];

	# Get exact results
	@expected = ();
	foreach (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			WITH top AS (
				SELECT v $operator '$_' AS distance FROM tst ORDER BY distance LIMIT $limit
			)
			SELECT i FROM tst WHERE (v $operator '$_') <= (SELECT MAX(distance) FROM top)
		));
		push(@expected, $res);
	}

	# Build index serially
	$node->safe_psql("postgres", qq(
		SET max_parallel_maintenance_workers = 0;
		CREATE INDEX idx ON tst USING ivfflat (v $opclass);
	));

	# Test approximate results
	if ($operator ne "<#>")
	{
		# TODO Fix test (uniform random vectors all have similar inner product)
		test_recall(1, 0.71, $operator);
		test_recall(10, 0.95, $operator);
	}

	# Test probes equals lists
	if ($operator eq "<=>")
	{
		test_recall(100, 0.9925, $operator);
	}
	else
	{
		test_recall(100, 1.00, $operator);
	}

	$node->safe_psql("postgres", "DROP INDEX idx;");

	# Build index in parallel
	my ($ret, $stdout, $stderr) = $node->psql("postgres", qq(
		SET client_min_messages = DEBUG;
		SET min_parallel_table_scan_size = 1;
		CREATE INDEX idx ON tst USING ivfflat (v $opclass);
	));
	is($ret, 0, $stderr);
	like($stderr, qr/using \d+ parallel workers/);

	# Test approximate results
	if ($operator ne "<#>")
	{
		# TODO Fix test (uniform random vectors all have similar inner product)
		test_recall(1, 0.71, $operator);
		test_recall(10, 0.95, $operator);
	}

	# Test probes equals lists
	if ($operator eq "<=>")
	{
		test_recall(100, 0.9925, $operator);
	}
	else
	{
		test_recall(100, 1.00, $operator);
	}

	$node->safe_psql("postgres", "DROP INDEX idx;");
}

done_testing();
