use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 9;

my $node;
my @queries = ();
my @expected;
my $limit = 20;

sub test_recall
{
	my ($probes, $min, $operator) = @_;
	my $correct = 0;
	my $total = 0;

	for my $i (0 .. $#queries) {
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET ivfflat.probes = $probes;
			SELECT i FROM tst ORDER BY v $operator '$queries[$i]' LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);
		my %actual_set = map { $_ => 1 } @actual_ids;

		my @expected_ids = split("\n", $expected[$i]);

		foreach (@expected_ids) {
			if (exists($actual_set{$_})) {
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
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[random(), random(), random()] FROM generate_series(1, 100000) i;"
);

# Generate queries
for (1..20) {
	my $r1 = rand();
	my $r2 = rand();
	my $r3 = rand();
	push(@queries, "[$r1,$r2,$r3]");
}

# Check each index type
my @operators = ("<->", "<#>", "<=>");

foreach (@operators) {
	my $operator = $_;

	# Get exact results
	@expected = ();
	foreach (@queries) {
		my $res = $node->safe_psql("postgres", "SELECT i FROM tst ORDER BY v $operator '$_' LIMIT $limit;");
		push(@expected, $res);
	}

	# Add index
	my $opclass;
	if ($operator == "<->") {
		$opclass = "vector_l2_ops";
	} elsif ($operator == "<#>") {
		$opclass = "vector_ip_ops";
	} else {
		$opclass = "vector_cosine_ops";
	}
	$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v $opclass);");

	# Test approximate results
	test_recall(1, 0.75, $operator);
	test_recall(10, 0.95, $operator);
	test_recall(100, 1.0, $operator);
}
