use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vector;');
my $value = '[' . join(',', ('1') x 2000) . ']';
$node->safe_psql('postgres', qq(
 CREATE TABLE tst (id serial, v vector(2000)) WITH (autovacuum_enabled=false);
 INSERT INTO tst(v) SELECT '$value' FROM generate_series(1,4);
 CREATE INDEX tst_idx ON tst USING ivfflat (v vector_l2_ops) WITH (lists=1);
));
# Establish a full first page whose hint has already been moved back by VACUUM.
$node->safe_psql('postgres', 'DELETE FROM tst WHERE id=1;');
$node->safe_psql('postgres', 'VACUUM tst;');
$node->safe_psql('postgres', "INSERT INTO tst(v) VALUES ('$value');");
my $bytes = $node->safe_psql('postgres', q(SELECT pg_relation_size('tst_idx')));
for my $cycle (1 .. 5)
{
 $node->safe_psql('postgres', 'DELETE FROM tst;');
 $node->safe_psql('postgres', 'VACUUM tst;');
 $node->safe_psql('postgres', "INSERT INTO tst(v) SELECT '$value' FROM generate_series(1,4);");
 is($node->safe_psql('postgres', q(SELECT pg_relation_size('tst_idx'))), $bytes, 'reuses space after equal-valued hint cleanup');
}
# A later cleanup must not move past pages already emptied by an earlier one.
$node->safe_psql('postgres', 'DELETE FROM tst WHERE id <> (SELECT max(id) FROM tst);');
$node->safe_psql('postgres', 'VACUUM tst;');
$node->safe_psql('postgres', 'DELETE FROM tst;');
$node->safe_psql('postgres', 'VACUUM tst;');
$node->safe_psql('postgres', "INSERT INTO tst(v) SELECT '$value' FROM generate_series(1,4);");
is($node->safe_psql('postgres', q(SELECT pg_relation_size('tst_idx'))), $bytes, 'later cleanup retains already-empty earlier pages');

# Exercise the new shared/exclusive lock ordering with concurrent insertion,
# cleanup, and reads; failures and deadlocks must surface as pgbench errors.
$node->pgbench('--no-vacuum --client=4 --transactions=200', 0,
 [qr{actually processed}], [qr{^$}], 'concurrent hint updates', {
 '049_insert@5' => "INSERT INTO tst(v) VALUES ('$value');",
 '049_delete@5' => 'DELETE FROM tst WHERE id=(SELECT min(id) FROM tst);',
 '049_vacuum@1' => 'VACUUM tst;',
 '049_scan@5' => "SELECT id FROM tst ORDER BY v <-> '$value' LIMIT 5;"
 });
my $heap_count = $node->safe_psql('postgres', 'SELECT count(*) FROM tst;');
is($node->safe_psql('postgres', qq(
 SET enable_seqscan=off; SET ivfflat.probes=1;
 SELECT count(*) FROM (SELECT id FROM tst ORDER BY v <-> '$value') q;
)), $heap_count, 'full-probe scan retains all rows');
done_testing();
