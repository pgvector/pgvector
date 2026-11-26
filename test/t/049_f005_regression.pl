use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vector;');


$node->safe_psql('postgres', q(
 CREATE TABLE tst (id int, v vector(8));
 INSERT INTO tst VALUES (1,'[-1,0,0,0,0,0,0,0]'),
 (2,'[1e20,1e20,1e20,1e20,1e20,1e20,1e20,1e20]');
));
my $query = q(SELECT id FROM tst ORDER BY v <#> '[1e20,-1e20,1e20,-1e20,1e20,-1e20,1e20,-1e20]');
for my $attempt (1 .. 30)
{
 $node->safe_psql('postgres', 'CREATE INDEX tst_idx ON tst USING hnsw (v vector_ip_ops) WITH (m=8,ef_construction=64);');
 for my $mode ('off', 'strict_order', 'relaxed_order')
 {
  my $settings = "SET enable_seqscan=off; SET enable_sort=off; SET hnsw.ef_search=1000; SET hnsw.iterative_scan=$mode;";
  like($node->safe_psql('postgres', "$settings EXPLAIN (COSTS OFF) $query"), qr/Index Scan using tst_idx/, 'index used') if $attempt == 1;
  is($node->safe_psql('postgres', "$settings $query"), "1\n2", "build $attempt, $mode: finite before NaN");
 }
 $node->safe_psql('postgres', 'DROP INDEX tst_idx;');
}

done_testing();
