use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table and force parallel execution
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst_empty (id int, v vector(3));");
# Insert enough rows to make parallel scan look attractive
$node->safe_psql("postgres", "INSERT INTO tst_empty SELECT i, '[1,2,3]' FROM generate_series(1, 1000000) i;");

# Force parallel scanning and aggregation even for subsets
$node->safe_psql("postgres", "ALTER TABLE tst_empty SET (parallel_workers = 4);");

my $gucs = "SET max_parallel_workers_per_gather = 4; SET min_parallel_table_scan_size = 0; SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0;";
my $query = "SELECT avg(v) FROM tst_empty WHERE id < 0;";

# Verify it performs a Partial Aggregate
my $explain = $node->safe_psql("postgres", "$gucs EXPLAIN $query");
like($explain, qr/Partial Aggregate/, "Query uses Parallel Aggregation");

# Execute the query on empty dataset
my ($ret, $out, $err) = $node->psql("postgres", "$gucs $query");

is($err, "", "Query should not fail due to CheckDim(0) on empty response from parallel workers");
is($out, "", "Average of 0 vectors is NULL");

done_testing();
