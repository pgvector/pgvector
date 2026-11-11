use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector(3));");
$node->safe_psql("postgres", "SELECT setseed(0.5);");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[random(), random(), random()] FROM generate_series(1, 20000) i;"
);

my $query = "[0.1,0.2,0.3]";

# Default is 0 (auto)
is($node->safe_psql("postgres", "LOAD 'vector'; SHOW ivfflat.probes"), "0", "default probes");

# Check each number of lists
foreach my $case ([100, 10], [1000, 32])
{
	my ($lists, $expected) = @$case;

	$node->safe_psql("postgres", "DROP INDEX IF EXISTS idx;");
	$node->safe_psql("postgres",
		"SET maintenance_work_mem = '256MB'; CREATE INDEX idx ON tst USING ivfflat (v vector_l2_ops) WITH (lists = $lists);");

	my $default_results = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SELECT i FROM tst ORDER BY v <-> '$query' LIMIT 10;
	));
	my $explicit_results = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET ivfflat.probes = $expected;
		SELECT i FROM tst ORDER BY v <-> '$query' LIMIT 10;
	));
	is($default_results, $explicit_results,
		"default matches $expected probes with lists = $lists");

	my $default_plan = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		EXPLAIN SELECT i FROM tst ORDER BY v <-> '$query' LIMIT 10;
	));
	my $explicit_plan = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET ivfflat.probes = $expected;
		EXPLAIN SELECT i FROM tst ORDER BY v <-> '$query' LIMIT 10;
	));
	is($default_plan, $explicit_plan,
		"default cost matches $expected probes with lists = $lists");
}

# Negative values are rejected
my ($ret, $stdout, $stderr) = $node->psql("postgres", "LOAD 'vector'; SET ivfflat.probes = -1;");
isnt($ret, 0, "negative probes rejected");
like($stderr, qr/outside the valid range/, "negative probes error");

done_testing();
