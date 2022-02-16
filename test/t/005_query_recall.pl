use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 60;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4 primary key, v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[random(), random(), random()] FROM generate_series(1, 100000) i;"
);

# Check each index type
my @operators = ("<->", "<#>", "<=>");
foreach (@operators) {
	my $operator = $_;

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

	# Test 100% recall
	for (1..20) {
		my $i = int(rand() * 100000);
		my $query = $node->safe_psql("postgres", "SELECT v FROM tst WHERE i = $i;");
		my $res = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SELECT v FROM tst ORDER BY v <-> '$query' LIMIT 1;
		));
		is($res, $query);
	}
}
