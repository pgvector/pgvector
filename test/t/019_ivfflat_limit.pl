use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[random(), random(), random()] FROM generate_series(1, 1000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 10);");

# Test limit
my $explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 100;
));
like($explain, qr/Index Scan/);

# Test limit + offset
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 90 OFFSET 10;
));
like($explain, qr/Index Scan/);

# Test limit with probes
$explain = $node->safe_psql("postgres", qq(
	SET ivfflat.probes = 2;
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 200;
));
like($explain, qr/Index Scan/);

# Test limit > expected tuples
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 101;
));
like($explain, qr/Seq Scan/);

# Test limit + offset > expected tuples
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 91 OFFSET 10;
));
like($explain, qr/Seq Scan/);

# Test limit > expected tuples with probes
$explain = $node->safe_psql("postgres", qq(
	SET ivfflat.probes = 2;
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 201;
));
like($explain, qr/Seq Scan/);

# Test no limit
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]';
));
like($explain, qr/Seq Scan/);

done_testing();
