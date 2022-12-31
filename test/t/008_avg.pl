use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 5;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (r1 real, r2 real, r3 real, v vector(3));");
$node->safe_psql("postgres", qq(
	INSERT INTO tst SELECT r1, r2, r3, ARRAY[r1, r2, r3] FROM (
		SELECT random() + 1.01 AS r1, random() + 2.01 AS r2, random() + 3.01 AS r3 FROM generate_series(1, 1000000) t
	) i;
));

# Test avg
my $avg = $node->safe_psql("postgres", "SELECT AVG(v) FROM tst;");
like($avg, qr/\[1\.5/);
like($avg, qr/,2\.5/);
like($avg, qr/,3\.5/);

# Test matches real
my $r1 = $node->safe_psql("postgres", "SELECT AVG(r1)::float4 FROM tst;");
my $r2 = $node->safe_psql("postgres", "SELECT AVG(r2)::float4 FROM tst;");
my $r3 = $node->safe_psql("postgres", "SELECT AVG(r3)::float4 FROM tst;");
is($avg, "[$r1,$r2,$r3]");

# Test explain
my $explain = $node->safe_psql("postgres", "EXPLAIN SELECT AVG(v) FROM tst;");
like($explain, qr/Partial Aggregate/);
