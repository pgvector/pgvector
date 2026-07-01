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
$node->safe_psql("postgres", "DELETE FROM tst");

# Test HNSW_SCAN_LOCK at the beginning of MarkDeleted is effective
$node->pgbench(
    "--no-vacuum --client=5 --transactions=1000",
    0,
    [qr{actually processed}],
    [qr{^$}],
    "concurrent SELECTs and VACUUM",
    {
        "046_hnsw_vacuum_scan_select\@1000" => "SELECT i FROM tst ORDER BY v <-> '[0,0,0]' LIMIT 10;",
        "046_hnsw_vacuum_scan_vacuum\@1" => "VACUUM tst;"
    }
);

done_testing();
