use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $dim = 3;
my $nc = 50;
my $limit = 20;

my $array_sql = join(",", ('random()') x $dim);

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim), c int4, t text);");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql], i % $nc, 'test ' || i FROM generate_series(1, 10000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX idx ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 100);");
$node->safe_psql("postgres", "ANALYZE tst;");

# Generate query
my @r = ();
for (1 .. $dim)
{
	push(@r, rand());
}
my $query = "[" . join(",", @r) . "]";
my $c = int(rand() * $nc);

# Test attribute filtering
my $explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c = $c ORDER BY v <-> '$query' LIMIT $limit;
));
# TODO Do not use index
like($explain, qr/Index Scan using idx/);

# Test attribute filtering with few rows removed
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c != $c ORDER BY v <-> '$query' LIMIT $limit;
));
like($explain, qr/Index Scan using idx/);

# Test attribute filtering with few rows removed comparison
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c >= 1 ORDER BY v <-> '$query' LIMIT $limit;
));
like($explain, qr/Index Scan using idx/);

# Test attribute filtering with many rows removed comparison
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c < 1 ORDER BY v <-> '$query' LIMIT $limit;
));
# TODO Do not use index
like($explain, qr/Index Scan using idx/);

# Test attribute filtering with few rows removed like
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE t LIKE '%%test%%' ORDER BY v <-> '$query' LIMIT $limit;
));
like($explain, qr/Index Scan using idx/);

# Test attribute filtering with many rows removed like
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE t LIKE '%%other%%' ORDER BY v <-> '$query' LIMIT $limit;
));
like($explain, qr/Seq Scan/);

# Test distance filtering
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE v <-> '$query' < 1 ORDER BY v <-> '$query' LIMIT $limit;
));
like($explain, qr/Index Scan using idx/);

# Test distance filtering greater than distance
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE v <-> '$query' > 1 ORDER BY v <-> '$query' LIMIT $limit;
));
# TODO Do not use index
like($explain, qr/Index Scan using idx/);

# Test distance filtering without order
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE v <-> '$query' < 1;
));
like($explain, qr/Seq Scan/);

# Test distance filtering without limit
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE v <-> '$query' < 1 ORDER BY v <-> '$query';
));
like($explain, qr/Seq Scan/);

# Test attribute index
$node->safe_psql("postgres", "CREATE INDEX attribute_idx ON tst (c);");
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c = $c ORDER BY v <-> '$query' LIMIT $limit;
));
# TODO Use attribute index
like($explain, qr/Index Scan using idx/);

# Test partial index
$node->safe_psql("postgres", "CREATE INDEX partial_idx ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 5) WHERE (c = $c);");
$explain = $node->safe_psql("postgres", qq(
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c = $c ORDER BY v <-> '$query' LIMIT $limit;
));
like($explain, qr/Index Scan using partial_idx/);

done_testing();
