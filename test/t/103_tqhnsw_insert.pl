use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my @queries = ();
my @expected;
my $limit = 20;
my $dim = 16;
my $array_sql = join(",", ("random() - 0.5") x $dim);

# ---------------------------------------------------------------------------
# test_recall -- run all queries against the tqhnsw index, compute recall@K.
#
# $min      -- minimum acceptable recall fraction
# $operator -- distance operator (<->, <#>, <=>)
# ---------------------------------------------------------------------------
sub test_recall
{
	my ($min, $operator) = @_;
	my $correct = 0;
	my $total = 0;

	# Verify the planner actually uses the index.
	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET tqhnsw.ef_search = 100;
		SET tqhnsw.rerank = 100;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v $operator '$queries[0]' LIMIT $limit;
	));
	like($explain, qr/Index Scan using idx on tst/, "index scan used for $operator");

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqhnsw.ef_search = 100;
			SET tqhnsw.rerank = 100;
			SELECT i FROM tst ORDER BY v $operator '$queries[$i]' LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);
		my %actual_set = map { $_ => 1 } @actual_ids;

		my @expected_ids = split("\n", $expected[$i]);

		foreach (@expected_ids)
		{
			if (exists($actual_set{$_}))
			{
				$correct++;
			}
			$total++;
		}
	}

	cmp_ok($correct / $total, ">=", $min, $operator);
}

# Initialize node
$node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i serial, v vector($dim));");

# Generate query vectors.  No srand() so they vary per run; ground truth is
# recomputed via seqscan each run, keeping recall assertions valid.
for (1 .. 20)
{
	my @coords = map { rand() - 0.5 } (1 .. $dim);
	push(@queries, "[" . join(",", @coords) . "]");
}

# Check each index type.  tqhnsw supports L2, inner-product, and cosine only —
# L1 (<+>) is not implemented.
my @operators = ("<->", "<#>", "<=>");
my @opclasses = ("vector_l2_ops", "vector_ip_ops", "vector_cosine_ops");

for my $i (0 .. $#operators)
{
	my $operator = $operators[$i];
	my $opclass = $opclasses[$i];

	# Seed 200 base rows BEFORE building the index, so subsequent pgbench
	# inserts exercise aminsert (on-disk insert), not the build path.
	$node->safe_psql("postgres",
		"INSERT INTO tst (v) SELECT ARRAY[$array_sql]::vector($dim) FROM generate_series(1, 200);");

	# Build the index over the initial rows.
	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (v $opclass) WITH (m = 16, ef_construction = 64);");

	# Concurrent inserts via pgbench (10 clients, 200 transactions each).
	# This exercises the aminsert path under real concurrency.
	$node->pgbench(
		"--no-vacuum --client=10 --transactions=200",
		0,
		[qr{actually processed}],
		[qr{^$}],
		"concurrent INSERTs",
		{
			"103_tqhnsw_insert_$opclass" => "INSERT INTO tst (v) VALUES (ARRAY[$array_sql]::vector($dim));"
		}
	);

	# Get exact ground-truth results via seqscan after all inserts are done.
	@expected = ();
	foreach (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$_' LIMIT $limit;
		));
		push(@expected, $res);
	}

	# Recall threshold 0.90 mirrors the spike-GREEN quantized-HNSW bound.
	test_recall(0.90, $operator);

	$node->safe_psql("postgres", "DROP INDEX idx;");
	$node->safe_psql("postgres", "TRUNCATE tst;");
}

# ---------------------------------------------------------------------------
# WAL replay: insert rows, build index, insert more rows, crash the server,
# restart (forcing WAL replay), and confirm the index still answers queries.
# ---------------------------------------------------------------------------
$node->safe_psql("postgres",
	"INSERT INTO tst (v) SELECT ARRAY[$array_sql]::vector($dim) FROM generate_series(1, 200);");
$node->safe_psql("postgres",
	"CREATE INDEX idx ON tst USING tqhnsw (v vector_l2_ops) WITH (m = 16, ef_construction = 64);");
$node->safe_psql("postgres",
	"INSERT INTO tst (v) SELECT ARRAY[$array_sql]::vector($dim) FROM generate_series(1, 100);");

my $before = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	SET tqhnsw.ef_search = 100;
	SELECT count(*) FROM (SELECT i FROM tst ORDER BY v <-> '$queries[0]' LIMIT 10) s;
));

$node->stop('immediate');
$node->start;

my $after = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	SET tqhnsw.ef_search = 100;
	SELECT count(*) FROM (SELECT i FROM tst ORDER BY v <-> '$queries[0]' LIMIT 10) s;
));

is($after, $before, "index answers after WAL replay (crash-restart)");
is($after, "10", "WAL-replayed index returns full result set");

done_testing();
