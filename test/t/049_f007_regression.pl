use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vector;');


my $value = '{' . join(',', map { "$_:1" } (1 .. 1000)) . '}/1000000000';
$node->safe_psql('postgres', qq(
 CREATE TABLE tst (id int, v sparsevec(1000000000)) WITH (autovacuum_enabled=false);
 INSERT INTO tst VALUES (0,'$value');
 CREATE INDEX tst_idx ON tst USING hnsw (v sparsevec_l2_ops);
));
my $bytes = $node->safe_psql('postgres', q(SELECT pg_relation_size('tst_idx')));
for my $cycle (1 .. 20)
{
 $node->safe_psql('postgres', 'DELETE FROM tst;');
 $node->safe_psql('postgres', 'VACUUM (INDEX_CLEANUP ON) tst;');
 $node->safe_psql('postgres', "INSERT INTO tst VALUES ($cycle,'$value');");
 is($node->safe_psql('postgres', q(SELECT pg_relation_size('tst_idx'))), $bytes, "cycle $cycle reuses deleted pages");
 is($node->safe_psql('postgres', "SET enable_seqscan=off; SELECT id FROM tst ORDER BY v <-> '$value'::sparsevec"), "$cycle", 'only current row returned');
}

done_testing();
