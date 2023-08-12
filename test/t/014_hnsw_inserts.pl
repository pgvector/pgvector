use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

# Ensures elements and neighbors on both same and different pages
my $dim = 1900;

my $array_sql = join(",", ('random()') x $dim);

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v vector($dim));");
$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v vector_l2_ops);");

sub idx_scan
{
	# Stats do not update instantaneously
	# https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-STATS-VIEWS
	sleep(1);
	$node->safe_psql("postgres", "SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelid = 'tst_v_idx'::regclass;");
}

for my $i (1 .. 20)
{
	$node->pgbench(
		"--no-vacuum --client=10 --transactions=1",
		0,
		[qr{actually processed}],
		[qr{^$}],
		"concurrent INSERTs",
		{
			"014_hnsw_inserts_$i" => "INSERT INTO tst VALUES (ARRAY[$array_sql]);"
		}
	);

	my $count = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SELECT COUNT(*) FROM (SELECT v FROM tst ORDER BY v <-> (SELECT v FROM tst LIMIT 1)) t;
	));
	is($count, 10);

	$node->safe_psql("postgres", "TRUNCATE tst;");
}

is(idx_scan(), 20);

done_testing();
