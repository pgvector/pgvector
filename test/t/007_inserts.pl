use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 5;

my $dim = 768;

my $array_sql = join(",", ('random()') x $dim);

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[$array_sql] FROM generate_series(1, 10000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v);");

$node->pgbench(
	"--no-vacuum --client=5 --transactions=100",
	0,
	[qr{actually processed}],
	[qr{^$}],
	"concurrent INSERTs",
	{
		"007_inserts" => "INSERT INTO tst SELECT ARRAY[$array_sql] FROM generate_series(1, 10) i;"
	}
);

my $expected = 10000 + 5 * 100 * 10;

my $count = $node->safe_psql("postgres", "SELECT COUNT(*) FROM tst;");
is($count, $expected);

$count = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	SET ivfflat.probes = 100;
	SELECT COUNT(*) FROM (SELECT v FROM tst ORDER BY v <-> (SELECT v FROM tst LIMIT 1)) t;
));
is($count, $expected);
