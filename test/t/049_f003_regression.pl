use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vector;');


for my $type ('vector', 'halfvec')
{
 for my $tail ('0', $type eq 'halfvec' ? '5.960464477539063e-8' : '1.401298464324817e-45')
 {
  $node->safe_psql('postgres', qq(
   CREATE TABLE tst (id int PRIMARY KEY, v $type(2));
   INSERT INTO tst VALUES (1,'[1,0]'), (2,'[-1,$tail]');
   CREATE INDEX tst_idx ON tst USING ivfflat (v ${type}_cosine_ops) WITH (lists=1);
  ));
  for my $operation ('SELECT 1', 'REINDEX INDEX tst_idx', 'VACUUM FULL tst', 'CLUSTER tst USING tst_pkey')
  {
   $node->safe_psql('postgres', $operation);
   my $rows = $node->safe_psql('postgres', q(
    SET enable_seqscan=off; SET ivfflat.probes=1;
    SELECT id FROM tst ORDER BY v <=> '[1,0]';
   ));
   is($rows, "1\n2", "$type tail=$tail after $operation");
  }
  $node->safe_psql('postgres', 'DROP TABLE tst;');
 }
}

done_testing();
