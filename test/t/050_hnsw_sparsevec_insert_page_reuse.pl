use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $dim = 10000;
my $rows = 4000;
my $small_nnz = 200;
my $large_nnz = 1000;

sub index_pages
{
	my ($node) = @_;
	return $node->safe_psql("postgres", "SELECT pg_relation_size('idx') / 8192;");
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

my $pages = index_pages($node);

# Delete scattered rows and vacuum
$node->safe_psql("postgres", "DELETE FROM tst WHERE i % 20 = 0;");
$node->safe_psql("postgres", "VACUUM tst;");
my $deleted = $rows / 20;

# Insert large rows that do not fit freed slots
$node->safe_psql("postgres",
	"INSERT INTO tst (v) SELECT make_sv($large_nnz, 900000 + i) FROM generate_series(1, 3) i;"
);
my $pages_before = index_pages($node);

# Insert small rows that fit freed slots
$node->safe_psql("postgres",
	"INSERT INTO tst (v) SELECT make_sv($small_nnz, 910000 + i) FROM generate_series(1, $deleted) i;"
);
my $growth = index_pages($node) - $pages_before;

# Compare to growth without reuse
my $no_reuse = $pages * $deleted / $rows;
note("index pages: $pages, growth: $growth, without reuse: $no_reuse");
cmp_ok($growth, '<', $no_reuse / 4, "small inserts after large inserts reuse freed slots");

done_testing();
