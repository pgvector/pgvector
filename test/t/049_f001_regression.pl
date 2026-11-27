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
 CREATE INDEX tst_idx ON tst USING ivfflat (v vector_ip_ops) WITH (lists=1);
 INSERT INTO tst VALUES
 (1,'[1e20,1e20,1e20,1e20,1e20,1e20,1e20,1e20]'),
 (2,'[1,0,0,0,0,0,0,0]'), (3,'[0,1,0,0,0,0,0,0]');
));
my $query = q(SELECT id FROM tst ORDER BY v <#> '[1e20,-1e20,1e20,-1e20,1e20,-1e20,1e20,-1e20]');
my $settings = 'SET enable_seqscan=off; SET enable_sort=off; SET ivfflat.probes=1;';
like($node->safe_psql('postgres', "$settings EXPLAIN (COSTS OFF) $query"), qr/Index Scan using tst_idx/, 'index used');
for my $memory ('4MB', '64kB')
{
 is($node->safe_psql('postgres', "$settings SET work_mem='$memory'; $query"), "2\n3\n1", "$memory: NaN last");
 is($node->safe_psql('postgres', "$settings SET work_mem='$memory'; $query LIMIT 2"), "2\n3", "$memory: LIMIT uses total order");
}
is($node->safe_psql('postgres', "SET enable_indexscan=off; $query"), "2\n3\n1", 'sequential sort agrees');

done_testing();
