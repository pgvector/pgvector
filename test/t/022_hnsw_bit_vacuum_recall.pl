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
	my ($min, $ef_search, $test_name) = @_;
	my $correct = 0;
	my $total = 0;

	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET hnsw.ef_search = $ef_search;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v <~> $queries[0] LIMIT $limit;
	));
	like($explain, qr/Index Scan/);

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET hnsw.ef_search = $ef_search;
			SELECT i FROM tst ORDER BY v <~> $queries[$i] LIMIT $limit;
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

	cmp_ok($correct / $total, ">=", $min, $test_name);
}

# Initialize node
$node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v bit($dim));");
$node->safe_psql("postgres", "ALTER TABLE tst SET (autovacuum_enabled = false);");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, (random() * $max)::bigint::bit($dim) FROM generate_series(1, 10000) i;"
);

# Add index
$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v bit_hamming_ops) WITH (m = 4, ef_construction = 8);");

# Delete data
$node->safe_psql("postgres", "DELETE FROM tst WHERE i > 2500;");

# Generate queries
for (1 .. 20)
{
	my $r = int(rand() * $max);
	push(@queries, "${r}::bigint::bit($dim)");
}

# Get exact results
@expected = ();
foreach (@queries)
{
	my $res = $node->safe_psql("postgres", qq(
		SET enable_indexscan = off;
		WITH top AS (
			SELECT v <~> $_ AS distance FROM tst ORDER BY distance LIMIT $limit
		)
		SELECT i FROM tst WHERE (v <~> $_) <= (SELECT MAX(distance) FROM top)
	));
	push(@expected, $res);
}

test_recall(0.35, 100, "before vacuum");

# TODO Test concurrent inserts with vacuum
$node->safe_psql("postgres", "VACUUM tst;");

test_recall(0.80, 100, "after vacuum");

done_testing();
