use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[i % 1000, i % 1000, i % 1000] FROM generate_series(1, 10000) i;"
);

my @limits = (128, 2048);
my @expected = ();

foreach (@limits) {
	my $res = $node->safe_psql("postgres", "SELECT i, v FROM tst ORDER BY v <-> '[0,0,0]', i LIMIT $_;");
	push(@expected, $res);
}

$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v) WITH (lists = 5);");

for my $i (0 .. $#limits) {
	my $res = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET ivfflat.probes = 5;
		WITH tmp AS (
			SELECT *, v <-> '[0,0,0]' AS d FROM tst ORDER BY v <-> '[0,0,0]' LIMIT $limits[$i]
		) SELECT i, v FROM tmp ORDER BY d, i;
	));
	is($res, $expected[$i]);
}
