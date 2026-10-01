use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $dim = 10000;
my $rows = 30000;
my $small_nnz = 200;
my $large_nnz = 1000;

# Buffers read by an insert, without keeping the row
sub insert_buffers
{
	my ($node, $nnz, $seed) = @_;
	my $explain = $node->safe_psql("postgres", qq(
		BEGIN;
		EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF)
		INSERT INTO tst (v) VALUES (make_sv($nnz, $seed));
		ROLLBACK;
	));
	my $buffers = 0;
	while ($explain =~ /shared hit=(\d+)(?: read=(\d+))?/g)
	{
		my $total = $1 + ($2 // 0);
		$buffers = $total if $total > $buffers;
	}
	return $buffers;
}

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i serial, v sparsevec($dim)) WITH (autovacuum_enabled = off);");

# Generate a sparsevec with the requested number of non-zeros
$node->safe_psql("postgres", qq(
	CREATE FUNCTION make_sv(nnz int, seed bigint) RETURNS sparsevec
	LANGUAGE sql IMMUTABLE AS \$\$
		SELECT ('{' || string_agg(
			((g - 1) * ($dim / nnz) + 1 + (seed % ($dim / nnz)))::text || ':' ||
			round((0.1 + ((seed * g) % 991)::numeric / 991.0), 4)::text,
			',' ORDER BY g) || '}/$dim')::sparsevec
		FROM generate_series(1, nnz) g;
	\$\$;
));

# Every row has a small number of non-zeros
$node->safe_psql("postgres",
	"INSERT INTO tst (v) SELECT make_sv($small_nnz, i) FROM generate_series(1, $rows) i;"
);
$node->safe_psql("postgres", "CREATE INDEX idx ON tst USING hnsw (v sparsevec_l2_ops);");

# Cost of a large insert while no element is deleted
my $baseline = insert_buffers($node, $large_nnz, 900001);

# Delete scattered rows and vacuum
$node->safe_psql("postgres", "DELETE FROM tst WHERE i % 20 = 0;");
$node->safe_psql("postgres", "VACUUM tst;");

my $pages = $node->safe_psql("postgres", "SELECT pg_relation_size('idx') / 8192;");

# Only the first large insert should scan past freed slots
insert_buffers($node, $large_nnz, 900002);
my $repeat = insert_buffers($node, $large_nnz, 900003);

# Insert a small row, then a large one
insert_buffers($node, $small_nnz, 900004);
my $after_small = insert_buffers($node, $large_nnz, 900005);

note("index pages: $pages, baseline: $baseline, repeat: $repeat, after small: $after_small");

# Compare to index size
my $limit = $pages / 2;
cmp_ok($repeat, '<', $limit, "large insert after vacuum does not scan the index");
cmp_ok($after_small, '<', $limit, "large insert after a small insert does not scan the index");

done_testing();
