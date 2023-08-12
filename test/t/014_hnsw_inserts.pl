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
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[$array_sql] FROM generate_series(1, 100) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v vector_l2_ops);");

$node->pgbench(
	"--no-vacuum --client=5 --transactions=100",
	0,
	[qr{actually processed}],
	[qr{^$}],
	"concurrent INSERTs",
	{
		"014_hnsw_inserts" => "INSERT INTO tst SELECT ARRAY[$array_sql] FROM generate_series(1, 10) i;"
	}
);

sub idx_scan
{
	# Stats do not update instantaneously
	# https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-STATS-VIEWS
	sleep(1);
	$node->safe_psql("postgres", "SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelid = 'tst_v_idx'::regclass;");
}

my $expected = 100 + 5 * 100 * 10;

my $count = $node->safe_psql("postgres", "SELECT COUNT(*) FROM tst;");
is($count, $expected);
is(idx_scan(), 0);

$count = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	SET hnsw.ef_search = 400;
	SELECT COUNT(*) FROM (SELECT v FROM tst ORDER BY v <-> (SELECT v FROM tst LIMIT 1)) t;
));
is($count, 400);
is(idx_scan(), 1);

done_testing();
