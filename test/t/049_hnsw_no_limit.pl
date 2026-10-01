use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Scans return a limited number of tuples, so the index should not be used
# when the planner expects every row to be fetched (tuple_fraction = 0)
# https://github.com/pgvector/pgvector/issues/846

my $dim = 3;
my $limit = 10;

my $array_sql = join(",", ('random()') x $dim);

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create tables and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim), c int4);");
$node->safe_psql("postgres", "CREATE TABLE cat (i int4 PRIMARY KEY, t text);");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql], i % 10 FROM generate_series(1, 10000) i;"
);
$node->safe_psql("postgres", "INSERT INTO cat SELECT i, 'cat ' || i FROM generate_series(0, 9) i;");
$node->safe_psql("postgres", "CREATE INDEX idx ON tst USING hnsw (v vector_l2_ops);");
$node->safe_psql("postgres", "ANALYZE;");

# Generate query
my @r = ();
for (1 .. $dim)
{
	push(@r, rand());
}
my $query = "[" . join(",", @r) . "]";
my $dist = "v <-> '$query'";

# Disable sorts so the index is used whenever it is allowed
my $settings = "SET enable_sort = off;";

sub sorted
{
	my ($res) = @_;
	return join("\n", sort { $a cmp $b } split("\n", $res));
}

# Check the index is not used and all rows are returned
sub test_no_index
{
	my ($name, $sql) = @_;

	my $explain = $node->safe_psql("postgres", "$settings EXPLAIN $sql;");
	unlike($explain, qr/Index Scan using idx/, "$name: no index scan");

	my $expected = $node->safe_psql("postgres", "SET enable_indexscan = off; $sql;");
	my $actual = $node->safe_psql("postgres", "$settings $sql;");
	is(sorted($actual), sorted($expected), "$name: all rows");
}

# Check the index is used
sub test_index
{
	my ($name, $sql, $extra_settings) = @_;
	$extra_settings //= "";

	my $explain = $node->safe_psql("postgres", "$settings $extra_settings EXPLAIN $sql;");
	like($explain, qr/Index Scan using idx/, "$name: index scan");
}

# Test without limit
test_no_index("no limit", "SELECT i FROM tst ORDER BY $dist");

# Test subqueries without limit, which get tuple_fraction = 0 when the outer
# query has an aggregate, GROUP BY, HAVING, DISTINCT, ORDER BY, or join
test_no_index("aggregate", "SELECT COUNT(*) FROM (SELECT i FROM tst ORDER BY $dist) s");
test_no_index("group by", "SELECT c, COUNT(*) FROM (SELECT c FROM tst ORDER BY $dist) s GROUP BY c");
test_no_index("grouping sets",
	"SELECT c, COUNT(*) FROM (SELECT c FROM tst ORDER BY $dist) s GROUP BY ROLLUP (c)");
test_no_index("having", "SELECT COUNT(*) FROM (SELECT i FROM tst ORDER BY $dist) s HAVING COUNT(*) > 0");
test_no_index("distinct", "SELECT DISTINCT c FROM (SELECT c FROM tst ORDER BY $dist) s");
test_no_index("order by", "SELECT i FROM (SELECT i FROM tst ORDER BY $dist) s ORDER BY i LIMIT $limit");
test_no_index("join",
	"SELECT s.i, cat.t FROM (SELECT i, c FROM tst ORDER BY $dist) s INNER JOIN cat ON cat.i = s.c");

# Test subqueries without limit when the outer query only reads the first rows
test_no_index("order by distance",
	"SELECT i FROM (SELECT i, $dist AS d FROM tst ORDER BY d) s ORDER BY d LIMIT $limit");
test_no_index("join order by distance",
	"SELECT s.i, cat.t FROM (SELECT i, c, $dist AS d FROM tst ORDER BY d) s INNER JOIN cat ON cat.i = s.c ORDER BY s.d LIMIT $limit"
);

# Test queries that still use the index
test_index("limit", "SELECT i FROM tst ORDER BY $dist LIMIT $limit");
test_index("outer limit", "SELECT i FROM (SELECT i FROM tst ORDER BY $dist) s LIMIT $limit");
test_index("aggregate with limit", "SELECT COUNT(*) FROM (SELECT i FROM tst ORDER BY $dist LIMIT $limit) s");
test_index("order by distance with limit",
	"SELECT i FROM (SELECT i, $dist AS d FROM tst ORDER BY d LIMIT $limit) s ORDER BY d");
test_index("join with limit",
	"SELECT s.i, cat.t FROM (SELECT i, c, $dist AS d FROM tst ORDER BY d LIMIT $limit) s INNER JOIN cat ON cat.i = s.c ORDER BY s.d"
);
test_index("no limit with enable_seqscan off", "SELECT i FROM tst ORDER BY $dist", "SET enable_seqscan = off;");
test_index("subquery with enable_seqscan off",
	"SELECT i FROM (SELECT i, $dist AS d FROM tst ORDER BY d) s ORDER BY d LIMIT $limit",
	"SET enable_seqscan = off;");

done_testing();
