use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my @queries = ();
my $dim = 5;
my $array_sql = join(",", ('floor(random() * 4)::int - 2') x $dim);

# Initialize node
$node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[$array_sql] FROM generate_series(1, 10000) i;"
);

# Generate queries
for (1 .. 20)
{
	my @r = ();
	for (1 .. $dim)
	{
		push(@r, int(rand() * 4) - 2);
	}
	push(@queries, "[" . join(",", @r) . "]");
}

# Check each distance function
my @functions = ("l2_distance", "inner_product", "cosine_distance", "l1_distance");

for my $function (@functions)
{
	for my $query (@queries)
	{
		my $expected = $node->safe_psql("postgres", "SELECT $function(v, '$query') FROM tst");

		# Test halfvec
		my $actual = $node->safe_psql("postgres", "SELECT $function(v::halfvec, '$query'::vector::halfvec) FROM tst");
		is($expected, $actual, "halfvec $function");

		# Test sparsevec
		$actual = $node->safe_psql("postgres", "SELECT $function(v::sparsevec, '$query'::vector::sparsevec) FROM tst");
		is($expected, $actual, "sparsevec $function");
	}
}

done_testing();
