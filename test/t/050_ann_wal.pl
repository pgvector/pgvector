# Follow the existing access-method WAL tests, with an exact SQL oracle.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->start;
$primary->backup('ann_backup');
my $replica = PostgreSQL::Test::Cluster->new('replica');
$replica->init_from_backup($primary, 'ann_backup', has_streaming => 1);
$replica->start;

my $query = q(SELECT id FROM tst ORDER BY v <-> '[13.4,3,5]', id);

sub check_results
{
	my ($name) = @_;
	my $expected = $primary->safe_psql('postgres',
		"SET enable_indexscan = off; SET enable_bitmapscan = off; $query");
	my $actual = $primary->safe_psql('postgres', "SET enable_seqscan = off; $query");
	is($actual, $expected, "$name: exact primary results");
	$primary->wait_for_replay_catchup($replica->name);
	my $replayed = $replica->safe_psql('postgres', "SET enable_seqscan = off; $query");
	is($replayed, $expected, "$name: exact replay results");
}

$primary->safe_psql('postgres', q(
	CREATE EXTENSION vector;
	CREATE TABLE tst (id integer, v vector(3));
	INSERT INTO tst SELECT i, ARRAY[i * 0.123, i % 13, i % 17]::vector
		FROM generate_series(1, 2000) i;
	INSERT INTO tst VALUES (3000, NULL);
	CREATE INDEX tst_ann ON tst USING ann (v vector_l2_ops);
));
check_results('bulk build');
$primary->safe_psql('postgres', q(
	INSERT INTO tst SELECT i, ARRAY[i,1,2]::vector FROM generate_series(4000, 5000) i;
	UPDATE tst SET v = '[13.4,3,5]' WHERE id = 1;
	DELETE FROM tst WHERE id % 2 = 0;
));
$primary->safe_psql('postgres', 'VACUUM tst');
$primary->safe_psql('postgres', q(
	INSERT INTO tst SELECT i, ARRAY[i,1,2]::vector FROM generate_series(6000, 6500) i;
	BEGIN;
	INSERT INTO tst VALUES (9999, '[13.4,3,5]');
	ROLLBACK;
));
check_results('insert update vacuum reuse and abort');

# An immediate shutdown exercises WAL recovery rather than clean buffer flush.
$primary->stop('immediate');
$primary->start;
check_results('crash recovery');
$primary->safe_psql('postgres', 'REINDEX INDEX CONCURRENTLY tst_ann');
check_results('concurrent reindex');
$primary->safe_psql('postgres', q(
	TRUNCATE tst;
	INSERT INTO tst VALUES (1, '[13.4,3,5]'), (2, NULL);
));
check_results('truncate');
$replica->stop;
$primary->stop;
done_testing();
