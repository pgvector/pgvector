use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 3;

my $node;
my @queries = ();
my @expected = ();

sub test_recall
{
	my ($probes, $min) = @_;
	my $correct = 0;
	my $total = 0;

	for my $i (0 .. $#queries) {
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET ivfflat.probes = $probes;
			SELECT i FROM tst ORDER BY v <-> '$queries[$i]' LIMIT 10;
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

	cmp_ok($correct / $total, ">=", $min);
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

# Get exact results
foreach (@queries) {
	my $res = $node->safe_psql("postgres", "SELECT i FROM tst ORDER BY v <-> '$_' LIMIT 10;");
	push(@expected, $res);
}

# Add index
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v);");

# Test approximate results
test_recall(1, 0.8);
test_recall(10, 0.95);
test_recall(100, 1.0);
