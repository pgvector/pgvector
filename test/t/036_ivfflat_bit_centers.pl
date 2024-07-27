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
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v bit(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, floor(random() * 8)::bigint::bit(3) FROM generate_series(1, 100) i;"
);

my $counts = $node->safe_psql("postgres", "SELECT v, COUNT(*) FROM tst GROUP BY 1 ORDER BY 1");
my @rows = split("\n", $counts);
is(scalar(@rows), 8);

# Create index with more lists than distinct values
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v bit_hamming_ops) WITH (lists = 100);");

for my $row (@rows)
{
	my ($v, $count) = split(/\|/, $row);

	my $actual = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SELECT i FROM tst ORDER BY v <~> '$v' LIMIT 100;
	));
	my @actual_ids = split("\n", $actual);

	# Test results are always found
	is(scalar(@actual_ids), $count);
}

done_testing();
