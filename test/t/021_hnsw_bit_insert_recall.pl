use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my @queries = ();
my @expected;
my $limit = 20;
my $dim = 52;
my $max = 2**$dim;

sub test_recall
{
	my ($min, $operator) = @_;
	my $correct = 0;
	my $total = 0;

	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET hnsw.ef_search = 100;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v $operator $queries[0] LIMIT $limit;
	));
	like($explain, qr/Index Scan/);

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET hnsw.ef_search = 100;
			SELECT i FROM tst ORDER BY v $operator $queries[$i] LIMIT $limit;
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
$node->safe_psql("postgres", "CREATE TABLE tst (i serial, v bit($dim));");

# Generate queries
for (1 .. 20)
{
	my $r = int(rand() * $max);
	push(@queries, "${r}::bigint::bit($dim)");
}

# Check each index type
my @operators = ("<~>", "<\%>");
my @opclasses = ("bit_hamming_ops", "bit_jaccard_ops");

for my $i (0 .. $#operators)
{
	my $operator = $operators[$i];
	my $opclass = $opclasses[$i];

	# Add index
	$node->safe_psql("postgres", "CREATE INDEX idx ON tst USING hnsw (v $opclass);");

	# Use concurrent inserts
	$node->pgbench(
		"--no-vacuum --client=10 --transactions=1000",
		0,
		[qr{actually processed}],
		[qr{^$}],
		"concurrent INSERTs",
		{
			"023_hnsw_bit_insert_recall_$opclass" => "INSERT INTO tst (v) VALUES ((random() * $max)::bigint::bit($dim));"
		}
	);

	# Get exact results
	@expected = ();
	foreach (@queries)
	{
		# Handle ties
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			WITH top AS (
				SELECT v $operator $_ AS distance FROM tst ORDER BY distance LIMIT $limit
			)
			SELECT i FROM tst WHERE (v $operator $_) <= (SELECT MAX(distance) FROM top)
		));
		push(@expected, $res);
	}

	# Test approximate results
	my $min = $operator eq "<\%>" ? 0.95 : 0.98;
	test_recall($min, $operator);

	$node->safe_psql("postgres", "DROP INDEX idx;");
	$node->safe_psql("postgres", "TRUNCATE tst;");
}

done_testing();
