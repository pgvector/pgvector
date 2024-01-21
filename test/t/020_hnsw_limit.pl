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
$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v vector_l2_ops);");

# Test limit
my $explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 40;
));
like($explain, qr/Index Scan/);

# Test limit with CTE
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE WITH cte AS (SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 40) SELECT * FROM cte;
));
like($explain, qr/Index Scan/);

# Test limit + offset
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 30 OFFSET 10;
));
like($explain, qr/Index Scan/);

# Test limit > ef_search
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 41;
));
like($explain, qr/Seq Scan/);

# Test limit > ef_search with CTE
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE WITH cte AS (SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 41) SELECT * FROM cte;
));
like($explain, qr/Seq Scan/);

# Test limit + offset > ef_search
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 31 OFFSET 10;
));
like($explain, qr/Seq Scan/);

# Test no limit
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT * FROM tst ORDER BY v <-> '[1,2,3]';
));
like($explain, qr/Seq Scan/);

done_testing();
