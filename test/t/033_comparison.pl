use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my $array_sql = join(",", ('floor(random() * 2)::int - 1') x 3);

# Initialize node
$node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v real[]);");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[$array_sql] FROM generate_series(1, 10000) i;"
);

for (1 .. 50)
{
	# Generate query
	my @r = ();
	for (1 .. (int(rand() * 3) + 2))
	{
		push(@r, int(rand() * 2) - 1);
	}
	my $query = "{" . join(",", @r) . "}";

	# Get expected result
	my $expected = $node->safe_psql("postgres", "SELECT btarraycmp(v, '$query') FROM tst");

	# Test vector
	my $actual = $node->safe_psql("postgres", "SELECT vector_cmp(v::vector, '$query'::real[]::vector) FROM tst");
	is($expected, $actual);

	# Test halfvec
	$actual = $node->safe_psql("postgres", "SELECT halfvec_cmp(v::halfvec, '$query'::real[]::halfvec) FROM tst");
	is($expected, $actual);

	# Test sparsevec
	$actual = $node->safe_psql("postgres", "SELECT sparsevec_cmp(v::vector::sparsevec, '$query'::real[]::vector::sparsevec) FROM tst");
	is($expected, $actual);
}

done_testing();
