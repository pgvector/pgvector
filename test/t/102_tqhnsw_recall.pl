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

# Build a SQL fragment for an inline random vector of $dim dimensions.
# Each coordinate is random()-0.5.  This produces a distinct vector per row
# because random() is called $dim times per row in the outer SELECT.
my $array_sql = join(",", ("random() - 0.5") x $dim);

# ---------------------------------------------------------------------------
# test_recall -- run all queries against the tqhnsw index, compute recall@K.
#
# $ef       -- value to SET tqhnsw.ef_search = N before each query
# $rerank   -- value to SET tqhnsw.rerank = N before each query
# $min      -- minimum acceptable recall fraction
# $operator -- distance operator (<->, <#>, <=>)
# $label    -- descriptive label for the cmp_ok message
# ---------------------------------------------------------------------------
sub test_recall
{
	my ($ef, $rerank, $min, $operator, $label) = @_;
	my $correct = 0;
	my $total = 0;

	# Verify the planner actually uses the index.
	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET tqhnsw.ef_search = $ef;
		SET tqhnsw.rerank = $rerank;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v $operator '$queries[0]' LIMIT $limit;
	));
	like($explain, qr/Index Scan using idx on tst/, "index scan used for $label");

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqhnsw.ef_search = $ef;
			SET tqhnsw.rerank = $rerank;
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

	cmp_ok($correct / $total, ">=", $min, $label);
}

# ---------------------------------------------------------------------------
# Initialize node
# ---------------------------------------------------------------------------
$node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Use a fixed Perl RNG seed so query vectors are reproducible.
srand(20240101);

# ---------------------------------------------------------------------------
# Create table and insert random vectors.
# PostgreSQL random() is unseeded here, so the row data varies across runs.
# That is intentional: exact ground truth is recomputed via seqscan each run
# (before the index is built), so recall assertions remain valid regardless of
# which data were generated.  Query-vector reproducibility is handled by the
# Perl srand(20240101) call above.
# ---------------------------------------------------------------------------
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql]::vector($dim) FROM generate_series(1, 3000) i;");

# ---------------------------------------------------------------------------
# Generate 20 query vectors from the fixed Perl RNG.
# ---------------------------------------------------------------------------
for (1 .. 20)
{
	my @coords = map { rand() - 0.5 } (1 .. $dim);
	push(@queries, "[" . join(",", @coords) . "]");
}

# ===========================================================================
# Test configuration A: L2, high ef + high rerank (full coverage)
#   → expect recall ≥ 0.95  (mirrors the spike-GREEN quantized HNSW bound)
# ===========================================================================
{
	my $opclass  = "vector_l2_ops";
	my $operator = "<->";

	# Exact ground truth (seqscan).
	@expected = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		push(@expected, $res);
	}

	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (v $opclass) WITH (m = 16, ef_construction = 64);");

	# High ef + high rerank: near-exact recovery.
	test_recall(100, 200, 0.95, $operator, "L2 ef=100 rerank=200");

	# Moderate ef: reduced recall but still reasonable.
	test_recall(40, 100, 0.80, $operator, "L2 ef=40 rerank=100");

	# Low ef, no rerank: exercises the unreranked quantized path; loose bound.
	test_recall(10, 0, 0.20, $operator, "L2 ef=10 rerank=0 (low-ef unreranked path)");

	$node->safe_psql("postgres", "DROP INDEX idx;");
}

# ===========================================================================
# Test configuration B: cosine (<=>), high ef + high rerank
#   → expect recall ≥ 0.90  (mirrors the spike-GREEN cosine bound)
# ===========================================================================
{
	my $opclass  = "vector_cosine_ops";
	my $operator = "<=>";

	# Exact ground truth.
	@expected = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		push(@expected, $res);
	}

	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (v $opclass) WITH (m = 16, ef_construction = 64);");

	# High ef + high rerank.
	test_recall(100, 200, 0.90, $operator, "cosine ef=100 rerank=200");

	# Low ef: just exercise the path.
	test_recall(10, 100, 0.20, $operator, "cosine ef=10 rerank=100 (low-ef path)");

	$node->safe_psql("postgres", "DROP INDEX idx;");
}

# ===========================================================================
# Test configuration C: inner product (<#>), high ef + high rerank
#   → expect recall ≥ 0.90  (mirrors the spike-GREEN inner product bound)
# ===========================================================================
{
	my $opclass  = "vector_ip_ops";
	my $operator = "<#>";

	# Exact ground truth.
	@expected = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		push(@expected, $res);
	}

	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (v $opclass) WITH (m = 16, ef_construction = 64);");

	# High ef + high rerank.
	test_recall(100, 200, 0.90, $operator, "inner product ef=100 rerank=200");

	# Low ef: just exercise the path.
	test_recall(10, 100, 0.20, $operator, "inner product ef=10 rerank=100 (low-ef path)");

	$node->safe_psql("postgres", "DROP INDEX idx;");
}

done_testing();
