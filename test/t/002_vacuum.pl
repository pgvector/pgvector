use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i % 10, ARRAY[i % 1000, i % 333, i % 55] FROM generate_series(1, 100000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v);");

# Get size
my $size = $node->safe_psql("postgres", "SELECT pg_total_relation_size('tst_v_idx');");

# Delete all, vacuum, and insert same data
$node->safe_psql("postgres", "DELETE FROM tst;");
$node->safe_psql("postgres", "VACUUM tst;");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i % 10, ARRAY[i % 1000, i % 333, i % 55] FROM generate_series(1, 100000) i;"
);

# Check size
my $new_size = $node->safe_psql("postgres", "SELECT pg_total_relation_size('tst_v_idx');");
is($size, $new_size, "size does not change");
