use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vector;');


for my $value (65504, -65504)
{
 $node->safe_psql('postgres', qq(
  CREATE TABLE tst (id int PRIMARY KEY, v halfvec(1));
  INSERT INTO tst SELECT i, '[$value]' FROM generate_series(1,16390) i;
  CREATE INDEX tst_idx ON tst USING ivfflat (v halfvec_l2_ops) WITH (lists=328);
 ));
 for my $operation ('SELECT 1', 'REINDEX INDEX tst_idx', 'VACUUM FULL tst', 'CLUSTER tst USING tst_pkey', 'ALTER TABLE tst ALTER COLUMN id TYPE bigint')
 {
  $node->safe_psql('postgres', $operation);
  is($node->safe_psql('postgres', qq(
   SET enable_seqscan=off; SET ivfflat.probes=328;
   SELECT count(*) = 16390 AND count(DISTINCT id) = 16390 AND bool_and(d=0)
   FROM (SELECT id, v <-> '[$value]' AS d FROM tst ORDER BY v <-> '[$value]') q;
  )), 't', "$value after $operation");
 }
 $node->safe_psql('postgres', 'DROP TABLE tst;');
}

done_testing();
