use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i serial, v sparsevec(100000));");
$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v sparsevec_l2_ops);");

for (1 .. 3)
{
	for (1 .. 100)
	{
		my @elements;
		my %indices;
		for (1 .. int(rand() * 100))
		{
			my $index = int(rand() * (100000 - 1)) + 1;
			if (!exists($indices{$index}))
			{
				my $value = rand();
				push(@elements, "$index:$value");
				$indices{$index} = 1;
			}
		}
		my $embedding = "{" . join(",", @elements) . "}/100000";
		$node->safe_psql("postgres", "INSERT INTO tst (v) VALUES ('$embedding');");
	}

	$node->safe_psql("postgres", "DELETE FROM tst WHERE i % 2 = 0;");
	$node->safe_psql("postgres", "VACUUM tst;");
	is(1, 1);
}

done_testing();
