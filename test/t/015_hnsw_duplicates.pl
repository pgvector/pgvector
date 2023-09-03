use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(3));");

sub insert_vectors
{
	for my $i (1 .. 20)
	{
		$node->safe_psql("postgres", "INSERT INTO tst VALUES ('[1,1,1]');");
	}
}

sub test_duplicates
{
	my ($exp) = @_;

	my $res = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET hnsw.ef_search = 1;
		SELECT COUNT(*) FROM (SELECT * FROM tst ORDER BY v <-> '[1,1,1]') t;
	));
	is($res, $exp);
}

# Test duplicates with build
insert_vectors();
$node->safe_psql("postgres", "CREATE INDEX idx ON tst USING hnsw (v vector_l2_ops);");
test_duplicates(10);

# Reset
$node->safe_psql("postgres", "TRUNCATE tst;");

# Test duplicates with inserts
insert_vectors();
test_duplicates(10);

# Test fallback path for inserts
$node->pgbench(
	"--no-vacuum --client=5 --transactions=100",
	0,
	[qr{actually processed}],
	[qr{^$}],
	"concurrent INSERTs",
	{
		"015_hnsw_duplicates" => "INSERT INTO tst VALUES ('[1,1,1]');"
	}
);

# Reset
$node->safe_psql("postgres", "TRUNCATE tst;");

# Test deletes with index scan
$node->safe_psql("postgres", "INSERT INTO tst SELECT '[1,1,1]' FROM generate_series(1, 10) i;");
$node->safe_psql("postgres", "DELETE FROM tst WHERE ctid IN (SELECT ctid FROM tst ORDER BY random() LIMIT 5);");
for (1 .. 3)
{
	test_duplicates(5);
}

done_testing();
