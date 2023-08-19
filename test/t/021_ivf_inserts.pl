use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

my $dim = 8000;

my $array_sql = join(",", ('random()') x $dim);

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->append_conf('postgresql.conf', qq(ivf.create_sq8_index_enabled = on));
$node->append_conf('postgresql.conf', qq(maintenance_work_mem = 512MB));
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4 primary key, v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(1, 10000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivf (v vector_l2_ops) WITH (quantizer='SQ8');");

sub idx_scan
{
	# Stats do not update instantaneously
	# https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-STATS-VIEWS
	sleep(1);
	$node->safe_psql("postgres", "SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelid = 'tst_v_idx'::regclass;");
}

my $expected = 10000;

my $count = $node->safe_psql("postgres", "SELECT COUNT(*) FROM tst;");
is($count, $expected);
is(idx_scan(), 0);

$count = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	SET ivf.probes = 100;
	SELECT COUNT(*) FROM (SELECT v FROM tst ORDER BY v <-> (SELECT v FROM tst LIMIT 1)) t;
));
is($count, $expected);
is(idx_scan(), 1);

# Test recall
for (1..20) {
  my $i = int(rand() * 10000);
  my $query = $node->safe_psql("postgres", "SELECT v FROM tst WHERE i = $i;");
  my $res = $node->safe_psql("postgres", qq(
    SET enable_seqscan = off;
    SELECT v FROM tst ORDER BY v <-> '$query' LIMIT 1;
  ));
  is($res, $query);
}

done_testing();
