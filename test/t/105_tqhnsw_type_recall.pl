# Recall for tqhnsw over halfvec and sparsevec columns.
#
# Mirrors 102_tqhnsw_recall.pl's proven structure, but the indexed column is a
# halfvec / sparsevec derived from the same dense random vector.  TurboQuant
# densifies both back to a float array before quantizing, so recall should track
# the vector path (fp16 input adds only minor noise; the sparsevec rows here are
# fully dense, i.e. all coords nonzero).  Ground truth is recomputed per run via
# seqscan on the SAME typed column, so the recall bounds hold regardless of the
# (unseeded) random data.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my @queries = ();
my @expected;
my $limit = 10;
my $dim = 32;

# Inline random vector of $dim dims (random()-0.5 per coord → distinct per row).
my $array_sql = join(",", ("random() - 0.5") x $dim);

# ---------------------------------------------------------------------------
# test_recall -- recall@K against the tqhnsw index on column $col.
#   $qcast  -- suffix cast applied to the query literal (e.g. "::halfvec(32)")
#   $col    -- indexed column name (vh / vs)
# ---------------------------------------------------------------------------
sub test_recall
{
	my ($ef, $rerank, $min, $operator, $col, $qcast, $label) = @_;
	my $correct = 0;
	my $total = 0;

	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET tqhnsw.ef_search = $ef;
		SET tqhnsw.rerank = $rerank;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY $col $operator '$queries[0]'$qcast LIMIT $limit;
	));
	like($explain, qr/Index Scan using idx on tst/, "index scan used for $label");

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqhnsw.ef_search = $ef;
			SET tqhnsw.rerank = $rerank;
			SELECT i FROM tst ORDER BY $col $operator '$queries[$i]'$qcast LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);
		my %actual_set = map { $_ => 1 } @actual_ids;

		my @expected_ids = split("\n", $expected[$i]);

		foreach (@expected_ids)
		{
			$correct++ if exists($actual_set{$_});
			$total++;
		}
	}

	cmp_ok($correct / $total, ">=", $min, $label);
}

# ---------------------------------------------------------------------------
# compute_ground_truth -- exact top-K per query via seqscan on column $col.
# ---------------------------------------------------------------------------
sub compute_ground_truth
{
	my ($operator, $col, $qcast) = @_;
	@expected = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY $col $operator '$q'$qcast LIMIT $limit;
		));
		push(@expected, $res);
	}
}

# ---------------------------------------------------------------------------
# Initialize node + data.
# ---------------------------------------------------------------------------
$node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

srand(20240101);

$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres",
	"CREATE TABLE tst (i int4, v vector($dim), vh halfvec($dim), vs sparsevec($dim));");

# Compute one random vector per row, derive the halfvec/sparsevec from it so all
# three columns describe the same point (the sparsevec is dense: all coords kept).
$node->safe_psql("postgres", qq(
	INSERT INTO tst
	SELECT i, vv, vv::halfvec($dim), vv::sparsevec
	FROM (
		SELECT i, ARRAY[$array_sql]::vector($dim) AS vv
		FROM generate_series(1, 3000) i
	) s;
));

for (1 .. 20)
{
	my @coords = map { rand() - 0.5 } (1 .. $dim);
	push(@queries, "[" . join(",", @coords) . "]");
}

# ===========================================================================
# halfvec — L2 / cosine / inner product.  Same bounds as the vector path
# (fp16 input adds only minor noise).
# ===========================================================================
{
	my $qcast = "::halfvec($dim)";

	compute_ground_truth("<->", "vh", $qcast);
	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (vh halfvec_l2_ops) WITH (m = 16, ef_construction = 64);");
	test_recall(100, 200, 0.95, "<->", "vh", $qcast, "halfvec L2 ef=100 rerank=200");
	test_recall(10, 0, 0.20, "<->", "vh", $qcast, "halfvec L2 ef=10 rerank=0 (low-ef unreranked)");
	$node->safe_psql("postgres", "DROP INDEX idx;");

	compute_ground_truth("<=>", "vh", $qcast);
	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (vh halfvec_cosine_ops) WITH (m = 16, ef_construction = 64);");
	test_recall(100, 200, 0.90, "<=>", "vh", $qcast, "halfvec cosine ef=100 rerank=200");
	$node->safe_psql("postgres", "DROP INDEX idx;");

	compute_ground_truth("<#>", "vh", $qcast);
	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (vh halfvec_ip_ops) WITH (m = 16, ef_construction = 64);");
	test_recall(100, 200, 0.90, "<#>", "vh", $qcast, "halfvec inner product ef=100 rerank=200");
	$node->safe_psql("postgres", "DROP INDEX idx;");
}

# ===========================================================================
# sparsevec — L2 / cosine / inner product.  TQ densifies the (here dense)
# sparsevec, so recall tracks the vector path.  CREATE INDEX emits the
# densification NOTICE (harmless for safe_psql).
# ===========================================================================
{
	my $qcast = "::vector($dim)::sparsevec";

	compute_ground_truth("<->", "vs", $qcast);
	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (vs sparsevec_l2_ops) WITH (m = 16, ef_construction = 64);");
	test_recall(100, 200, 0.95, "<->", "vs", $qcast, "sparsevec L2 ef=100 rerank=200");
	test_recall(10, 0, 0.20, "<->", "vs", $qcast, "sparsevec L2 ef=10 rerank=0 (low-ef unreranked)");
	$node->safe_psql("postgres", "DROP INDEX idx;");

	compute_ground_truth("<=>", "vs", $qcast);
	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (vs sparsevec_cosine_ops) WITH (m = 16, ef_construction = 64);");
	test_recall(100, 200, 0.90, "<=>", "vs", $qcast, "sparsevec cosine ef=100 rerank=200");
	$node->safe_psql("postgres", "DROP INDEX idx;");

	compute_ground_truth("<#>", "vs", $qcast);
	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (vs sparsevec_ip_ops) WITH (m = 16, ef_construction = 64);");
	test_recall(100, 200, 0.90, "<#>", "vs", $qcast, "sparsevec inner product ef=100 rerank=200");
	$node->safe_psql("postgres", "DROP INDEX idx;");
}

done_testing();
