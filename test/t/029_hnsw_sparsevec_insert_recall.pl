use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my @queries = ();
my @expected;
my $limit = 20;
my $array_sql = join(",", ('random() * random()') x 3);

sub test_recall
{
	my ($min, $operator) = @_;
	my $correct = 0;
	my $total = 0;

	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v $operator '$queries[0]' LIMIT $limit;
	));
	like($explain, qr/Index Scan/);

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SELECT i FROM tst ORDER BY v $operator '$queries[$i]' LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);
		my %actual_set = map { $_ => 1 } @actual_ids;

		my @expected_ids = split("\n", $expected[$i]);

		foreach (@expected_ids)
		{
			if (exists($actual_set{$_}))
			{
				$correct++;
			}
			$total++;
		}
	}

	cmp_ok($correct / $total, ">=", $min, $operator);
}

# Initialize node
$node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i serial, v sparsevec(3));");

# Generate queries
for (1 .. 20)
{
	my $r1 = rand();
	my $r2 = rand();
	my $r3 = rand();
	push(@queries, "{1:$r1,2:$r2,3:$r3}/3");
}

# Check each index type
my @operators = ("<->", "<#>", "<=>", "<+>");
my @opclasses = ("sparsevec_l2_ops", "sparsevec_ip_ops", "sparsevec_cosine_ops", "sparsevec_l1_ops");

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
			"025_hnsw_sparsevec_insert_recall_$opclass" => "INSERT INTO tst (v) VALUES (ARRAY[$array_sql]::vector::sparsevec);"
		}
	);

	# Get exact results
	@expected = ();
	foreach (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$_' LIMIT $limit;
		));
		push(@expected, $res);
	}

	# Test approximate results
	my $min = $operator eq "<#>" ? 0.97 : 0.99;
	test_recall($min, $operator);

	$node->safe_psql("postgres", "DROP INDEX idx;");
	$node->safe_psql("postgres", "TRUNCATE tst;");
}

done_testing();
