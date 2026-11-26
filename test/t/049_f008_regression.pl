use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vector;');


# This also exercises unrelated allocations when built with pointer-pair sanitizers.
$node->safe_psql('postgres', q(
 CREATE TABLE tst (id int, v vector(3));
 INSERT INTO tst SELECT i, ARRAY[(i%23)-11,(i%19)-9,(i%17)-8]::vector FROM generate_series(1,5000) i;
));
for my $workers (0, 2)
{
 $node->safe_psql('postgres', qq(
  SET max_parallel_maintenance_workers=$workers;
  CREATE INDEX tst_idx ON tst USING hnsw (v vector_l2_ops);
  REINDEX INDEX tst_idx;
 ));
 is($node->safe_psql('postgres', q(
  SET enable_seqscan=off;
  SELECT count(*) FROM (SELECT id FROM tst ORDER BY v <-> '[0,0,0]' LIMIT 10) q;
 )), '10', "equal-distance candidates, workers=$workers");
 $node->safe_psql('postgres', 'DROP INDEX tst_idx;');
}

done_testing();
