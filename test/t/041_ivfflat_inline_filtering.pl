use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my @queries = ();
my @cs = ();
my @expected;
my $limit = 20;
my $dim = 3;
my $array_sql = join(",", ('random()') x $dim);
my $nc = 100;

sub test_recall
{
	my ($probes, $min, $operator) = @_;
	my $correct = 0;
	my $total = 0;

	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET ivfflat.probes = $probes;
		EXPLAIN ANALYZE SELECT i FROM tst WHERE c = $cs[0] ORDER BY v $operator '$queries[0]' LIMIT $limit;
	));
	like($explain, qr/Index Cond/);

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET ivfflat.probes = $probes;
			SELECT i FROM tst WHERE c = $cs[$i] ORDER BY v $operator '$queries[$i]' LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);
		my %actual_set = map { $_ => 1 } @actual_ids;

		is(scalar(@actual_ids), $limit);

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
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim), c int4);");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql], i % $nc FROM generate_series(1, 100000) i;"
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
	push(@cs, int(rand() * $nc));
}

# Get exact results
@expected = ();
for my $i (0 .. $#queries)
{
	my $res = $node->safe_psql("postgres", "SELECT i FROM tst WHERE c = $cs[$i] ORDER BY v <-> '$queries[$i]' LIMIT $limit;");
	push(@expected, $res);
}

# Add index
$node->safe_psql("postgres", qq(
	CREATE INDEX ON tst USING ivfflat (v vector_l2_ops, c) WITH (lists = 100);
));

# Test recall
test_recall(10, 0.99, '<->');

# Test vacuum
$node->safe_psql("postgres", "DELETE FROM tst WHERE c > 5;");
$node->safe_psql("postgres", "VACUUM tst;");

done_testing();
