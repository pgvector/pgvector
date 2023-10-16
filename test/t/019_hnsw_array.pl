use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

my $node;
my @queries = ();
my @expected;
my $limit = 20;

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
$node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v float4[3]);");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[random(), random(), random()] FROM generate_series(1, 10000) i;"
);

$node->safe_psql("postgres", qq(
	CREATE FUNCTION float4_l2_distance(float4[], float4[]) RETURNS float8
		AS 'BEGIN RETURN l2_distance(\$1::vector, \$2::vector); END;'
		LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

	CREATE FUNCTION float4_l2_squared_distance(float4[], float4[]) RETURNS float8
		AS 'BEGIN RETURN vector_l2_squared_distance(\$1::vector, \$2::vector); END;'
		LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

	CREATE OPERATOR <-> (
		LEFTARG = float4[], RIGHTARG = float4[], PROCEDURE = float4_l2_distance,
		COMMUTATOR = '<->'
	);

	CREATE OPERATOR CLASS float4_l2_ops
		FOR TYPE float4[] USING hnsw AS
		OPERATOR 1 <-> (float4[], float4[]) FOR ORDER BY float_ops,
		FUNCTION 1 float4_l2_squared_distance(float4[], float4[]);
));

# Generate queries
for (1 .. 20)
{
	my $r1 = rand();
	my $r2 = rand();
	my $r3 = rand();
	push(@queries, "{$r1,$r2,$r3}");
}

# Check each index type
my @operators = ("<->");
my @opclasses = ("float4_l2_ops");

for my $i (0 .. $#operators)
{
	my $operator = $operators[$i];
	my $opclass = $opclasses[$i];

	# Get exact results
	@expected = ();
	foreach (@queries)
	{
		my $res = $node->safe_psql("postgres", "SELECT i FROM tst ORDER BY v $operator '$_' LIMIT $limit;");
		push(@expected, $res);
	}

	# Add index
	$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v $opclass) WITH (dimensions = 3);");

	my $min = $operator eq "<#>" ? 0.80 : 0.99;
	test_recall($min, $operator);
}

done_testing();
