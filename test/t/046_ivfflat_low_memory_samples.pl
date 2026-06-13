use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[random(), random(), random()] FROM generate_series(1, 100000) i;"
);

my ($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET client_min_messages = NOTICE;
	SET maintenance_work_mem = '768kB';
	CREATE INDEX ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 200);
));
ok($ret, 'index build succeeds with low maintenance_work_mem');
like($stderr, qr/reducing ivfflat index samples from 10000 to \d+ due to maintenance_work_mem/);

done_testing();
