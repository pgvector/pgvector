use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector(3));");
$node->safe_psql("postgres",
  "INSERT INTO tst SELECT i, ARRAY[random(), random(), random()] FROM generate_series(1,100000) i;"
);

# Get exact results
my $expected = $node->safe_psql("postgres", "SELECT i FROM tst ORDER BY v <-> '[0.5,0.5,0.5]' LIMIT 10;");
my @expected_ids = split("\n", $expected);

# Add index
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v);");

# Get approximate results
$node->safe_psql("postgres", "SET enable_seqscan = off;");
my $actual = $node->safe_psql("postgres", "SELECT i FROM tst ORDER BY v <-> '[0.5,0.5,0.5]' LIMIT 10;");
my @actual_ids = split("\n", $actual);
my %actual_set = map { $_ => 1 } @actual_ids;

my $count = 0;
for my $el (@expected_ids) {
  if (exists($actual_set{$el})) {
    $count++;
  }
}

# TODO Run more queries to prevent flakiness
# TODO Test ivfflat.probes
cmp_ok($count, ">=", 5);
