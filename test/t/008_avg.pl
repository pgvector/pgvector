use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (r1 real, r2 real, r3 real, v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT r1, r2, r3, ARRAY[r1, r2, r3] FROM (
		SELECT random() + 1 AS r1, random() + 2 AS r2, random() + 3 AS r3 FROM generate_series(1, 1000000) t
	) i;"
);

# Test matches real
my $r1 = $node->safe_psql("postgres", "SELECT AVG(r1)::float4 FROM tst;");
my $r2 = $node->safe_psql("postgres", "SELECT AVG(r2)::float4 FROM tst;");
my $r3 = $node->safe_psql("postgres", "SELECT AVG(r3)::float4 FROM tst;");
my $avg = $node->safe_psql("postgres", "SELECT AVG(v) FROM tst;");
is($avg, "[$r1,$r2,$r3]");

# Test explain
my $explain = $node->safe_psql("postgres", "EXPLAIN SELECT AVG(v) FROM tst;");
like($explain, qr/Partial Aggregate/);
