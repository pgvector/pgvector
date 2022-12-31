use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 4;

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
