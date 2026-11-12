use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Test VACUUM for indexes where pages hold many elements and where they hold
# few elements. This exercises both the TidStore and hash table deletion lists
# on Postgres 17+.

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

$node->safe_psql("postgres", "CREATE EXTENSION vector;");

for my $dim (3, 500)
{
	my $table = "tst$dim";
	my $array_sql = join(", ", ('random()') x $dim);

	# Create table and index
	$node->safe_psql("postgres", "CREATE TABLE $table (i int4, v vector($dim));");
	$node->safe_psql("postgres", "ALTER TABLE $table SET (autovacuum_enabled = false);");
	$node->safe_psql("postgres",
		"INSERT INTO $table SELECT i, ARRAY[$array_sql] FROM generate_series(1, 2000) i;"
	);
	$node->safe_psql("postgres", "CREATE INDEX ON $table USING hnsw (v vector_l2_ops);");

	# Delete some elements and update others (the old heap TIDs become dead)
	$node->safe_psql("postgres", "DELETE FROM $table WHERE i % 3 = 0;");
	$node->safe_psql("postgres", "UPDATE $table SET v = v WHERE i % 5 = 0;");

	$node->safe_psql("postgres", "VACUUM $table;");

	# Check that index scans do not return deleted elements
	my $deleted = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET hnsw.ef_search = 100;
		SELECT count(*) FROM (
			SELECT i FROM $table ORDER BY v <-> (SELECT v FROM $table WHERE i = 1) LIMIT 10
		) s WHERE i % 3 = 0;
	));
	is($deleted, 0, "no deleted elements with $dim dimensions");

	# Check recall of index scans
	my $expected = $node->safe_psql("postgres", qq(
		SET enable_indexscan = off;
		SELECT string_agg(i::text, ',' ORDER BY i) FROM (
			SELECT i FROM $table ORDER BY v <-> (SELECT v FROM $table WHERE i = 1) LIMIT 10
		) s;
	));
	my @expected_ids = split(",", $expected);

	my $actual = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET hnsw.ef_search = 1000;
		SELECT string_agg(i::text, ',' ORDER BY i) FROM (
			SELECT i FROM $table ORDER BY v <-> (SELECT v FROM $table WHERE i = 1) LIMIT 10
		) s;
	));
	my %actual_set = map { $_ => 1 } split(",", $actual);

	my $correct = 0;
	foreach (@expected_ids)
	{
		$correct++ if (exists($actual_set{$_}));
	}

	cmp_ok($correct / scalar(@expected_ids), ">=", 0.9, "recall with $dim dimensions");
}

done_testing();
