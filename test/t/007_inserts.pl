use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 3;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(768));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT (SELECT array_agg(random()) FROM generate_series(1, 768)) FROM generate_series(1, 10000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v);");

$node->pgbench(
	"--no-vacuum --client=5 --transactions=100",
	0,
	[qr{actually processed}],
	[qr{^$}],
	"concurrent INSERTs",
	{
		"007_concurrent" => q(
			BEGIN;
			INSERT INTO tst SELECT (SELECT array_agg(random()) FROM generate_series(1, 768)) FROM generate_series(1, 10) i;
			COMMIT;
		),
	}
);
