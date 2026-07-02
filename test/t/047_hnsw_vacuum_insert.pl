use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $dim = 3;
my $array_sql = join(",", ('random()') x $dim);

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i serial, v vector($dim));");
$node->safe_psql("postgres", "ALTER TABLE tst SET (autovacuum_enabled = false);");
$node->safe_psql("postgres",
    "INSERT INTO tst (v) SELECT ARRAY[$array_sql] FROM generate_series(1, 1000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v vector_l2_ops);");

# Test no "hnsw graph not repaired" errors
$node->pgbench(
    "--no-vacuum --client=5 --transactions=1000",
    0,
    [qr{actually processed}],
    [qr{^$}],
    "concurrent INSERTs, DELETEs, SELECTs, and VACUUM",
    {
        "047_hnsw_vacuum_insert_insert\@500" => "INSERT INTO tst (v) VALUES (ARRAY[$array_sql]);",
        "047_hnsw_vacuum_insert_delete\@500" => "DELETE FROM tst WHERE i = (SELECT i FROM tst LIMIT 1);",
        "047_hnsw_vacuum_insert_select\@20" => "SELECT i FROM tst ORDER BY v <-> (SELECT ARRAY[$array_sql]::vector) LIMIT 10;",
        "047_hnsw_vacuum_insert_vacuum\@1" => "VACUUM tst;"
    }
);

done_testing();
