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
ok($ret, 'vector(3) index build succeeds with low maintenance_work_mem');
like($stderr, qr/reducing ivfflat index samples from 10000 to 735 due to maintenance_work_mem/);

# halfvec uses a smaller item size
$node->safe_psql("postgres", "CREATE TABLE tst_half (v halfvec(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst_half SELECT ARRAY[random(), random(), random()] FROM generate_series(1, 100000) i;"
);

($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET client_min_messages = NOTICE;
	SET maintenance_work_mem = '768kB';
	CREATE INDEX ON tst_half USING ivfflat (v halfvec_l2_ops) WITH (lists = 200);
));
ok($ret, 'halfvec(3) index build succeeds with low maintenance_work_mem');
like($stderr, qr/reducing ivfflat index samples from 10000 to 747 due to maintenance_work_mem/);

# Higher dimensions are common for embeddings
$node->safe_psql("postgres", "CREATE TABLE tst_large (v vector(1536));");
$node->safe_psql("postgres", qq(
	INSERT INTO tst_large
	SELECT (
		SELECT array_agg(random()) FROM generate_series(1, 1536)
	)::vector(1536)
	FROM generate_series(1, 1000) i;
));

($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET client_min_messages = NOTICE;
	SET maintenance_work_mem = '768kB';
	CREATE INDEX ON tst_large USING ivfflat (v vector_l2_ops) WITH (lists = 10);
));
ok($ret, 'vector(1536) index build succeeds with low maintenance_work_mem');
like($stderr, qr/reducing ivfflat index samples from 10000 to 96 due to maintenance_work_mem/);

# Extremely low maintenance_work_mem should fail fast without a misleading notice
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET client_min_messages = NOTICE;
	SET maintenance_work_mem = '1kB';
	CREATE INDEX lists200 ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 200);
));
ok(!$ret, 'extremely low maintenance_work_mem fails index build');
like($stderr, qr/memory required is/);
unlike($stderr, qr/reducing ivfflat index samples/);

done_testing();
