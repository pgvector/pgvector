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
# test_recall -- run all queries against the tqflat index, compute recall@K.
#
# $rerank   -- value to SET tqflat.rerank = N before each query
# $min      -- minimum acceptable recall fraction
# $operator -- distance operator (<->, <#>, <=>)
# $label    -- descriptive label for the cmp_ok message
# ---------------------------------------------------------------------------
sub test_recall
{
	my ($rerank, $min, $operator, $label) = @_;
	my $correct = 0;
	my $total = 0;

	# Verify the planner actually uses the index.
	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET tqflat.rerank = $rerank;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v $operator '$queries[0]' LIMIT $limit;
	));
	like($explain, qr/Index Scan using idx on tst/, "index scan used for $label");

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqflat.rerank = $rerank;
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
# Test configuration A: bits=4, L2, rerank=200  → expect recall ≥ 0.95
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
		"CREATE INDEX idx ON tst USING tqflat (v $opclass) WITH (bits = 4);");

	test_recall(200, 0.95, $operator, "bits=4 rerank=200 L2");

	$node->safe_psql("postgres", "DROP INDEX idx;");
}

# ===========================================================================
# Test configuration B: bits=4, L2, unreranked path
# (the v4 blocked layout supports only bits = 4, so the former bits=2
# configuration is gone; this block keeps the rerank=0 path coverage)
# ===========================================================================
{
	my $opclass  = "vector_l2_ops";
	my $operator = "<->";

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
		"CREATE INDEX idx ON tst USING tqflat (v $opclass) WITH (bits = 4);");

	# Assert the no-rerank path runs and returns something reasonable.
	# Threshold is loose (≥0.30) — it just proves the quantized path executes
	# and returns plausible results; rerank>0 (config A) significantly improves it.
	test_recall(0, 0.30, $operator, "bits=4 rerank=0 L2 (unreranked path)");

	$node->safe_psql("postgres", "DROP INDEX idx;");
}

# ===========================================================================
# Test configuration C: bits=4, cosine (<=>), rerank=200  → recall ≥ 0.90
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
		"CREATE INDEX idx ON tst USING tqflat (v $opclass) WITH (bits = 4);");

	test_recall(200, 0.90, $operator, "bits=4 rerank=200 cosine");

	$node->safe_psql("postgres", "DROP INDEX idx;");
}

# ===========================================================================
# Test configuration D: bits=4, inner product (<#>), rerank=200 → recall ≥ 0.90
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
		"CREATE INDEX idx ON tst USING tqflat (v $opclass) WITH (bits = 4);");

	test_recall(200, 0.90, $operator, "bits=4 rerank=200 inner product");

	$node->safe_psql("postgres", "DROP INDEX idx;");
}

done_testing();
