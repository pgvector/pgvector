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
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[1.01 + random(), 2.01 + random(), 3.01 + random()] FROM generate_series(1, 1000000) i;"
);

# Test avg
my $avg = $node->safe_psql("postgres", "SELECT AVG(v) FROM tst;");
like($avg, qr/\[1\.5/);
like($avg, qr/,2\.5/);
like($avg, qr/,3\.5/);

# Test explain
my $explain = $node->safe_psql("postgres", "EXPLAIN SELECT AVG(v) FROM tst;");
like($explain, qr/Partial Aggregate/);

# Test matches real
$node->safe_psql("postgres", "CREATE TABLE tst2 (r real, v vector(1));");
$node->safe_psql("postgres",
	"INSERT INTO tst2 SELECT t.r, ARRAY[t.r] FROM (SELECT random() AS r FROM generate_series(1, 1000000) t0) t;"
);
my $expected = $node->safe_psql("postgres", "SELECT AVG(r)::float4 FROM tst2;");
$avg = $node->safe_psql("postgres", "SELECT AVG(v) FROM tst2;");
is($avg, "[$expected]");
