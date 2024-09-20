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

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(1, 100000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v vector_l2_ops);");

my $count = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	SET ivfflat.probes = 10;
	SET ivfflat.streaming = on;
	SELECT COUNT(*) FROM (SELECT v FROM tst WHERE i % 10000 = 0 ORDER BY v <-> (SELECT v FROM tst LIMIT 1) LIMIT 11) t;
));
is($count, 10);

done_testing();
