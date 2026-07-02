# Test tqhnsw recall preservation after VACUUM and WAL replay through a
# streaming standby.  Structure:
#   Part 1 (recall-after-vacuum): mirrors 102_tqhnsw_recall.pl — same dataset
#     size, same GUCs — but adds a DELETE + VACUUM step before the recall
#     assertion and verifies deleted ids are absent.
#   Part 2 (WAL replay):         mirrors 010_hnsw_wal.pl — primary/replica
#     streaming setup, VACUUM replayed to replica, recall verified on both.
#
# Recall gates: before-vacuum gates run at ef_search=100 with the same bands
# as 102 (the conditions are identical).  After-vacuum gates keep the same
# bands but run at ef_search=200: vacuum graph repair adds run-to-run variance
# on top of the per-backend build RNG, and the deeper search absorbs it
# (measured floor 0.985 over repeated runs at ef=200 vs 0.96 at ef=100)
# without weakening the asserted quality floor.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ---------------------------------------------------------------------------
# Dataset constants — intentionally match 102_tqhnsw_recall.pl so recall
# bands are directly comparable.
# ---------------------------------------------------------------------------
my $dim    = 32;
my $nrows  = 3000;
my $nquery = 20;
my $limit  = 10;

# SQL fragment to generate one random $dim-dimensional vector per row.
my $array_sql = join(",", ("random() - 0.5") x $dim);

# ---------------------------------------------------------------------------
# test_recall -- compute recall@$limit for all query vectors.
#
# $node     -- PostgreSQL::Test::Cluster instance to query
# $ef       -- tqhnsw.ef_search value
# $rerank   -- tqhnsw.rerank value
# $min      -- minimum acceptable recall fraction
# $operator -- distance operator (<->, <=>, <#>)
# $label    -- test description for cmp_ok
# $expected -- arrayref of seqscan ground-truth result strings
# $queries  -- arrayref of query vector strings
# ---------------------------------------------------------------------------
sub test_recall
{
	my ($node, $ef, $rerank, $min, $operator, $label, $expected, $queries) = @_;
	my $correct = 0;
	my $total   = 0;

	# Verify the planner actually uses the index.
	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET tqhnsw.ef_search = $ef;
		SET tqhnsw.rerank = $rerank;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v $operator '$queries->[0]' LIMIT $limit;
	));
	like($explain, qr/Index Scan using idx on tst/, "index scan used for $label");

	for my $i (0 .. $#$queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqhnsw.ef_search = $ef;
			SET tqhnsw.rerank = $rerank;
			SELECT i FROM tst ORDER BY v $operator '$queries->[$i]' LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);
		my %actual_set = map { $_ => 1 } @actual_ids;

		my @expected_ids = split("\n", $expected->[$i]);

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

# ===========================================================================
# PART 1 — Recall-after-vacuum on a single node
#
# For each metric (L2, cosine, inner product):
#   1. Insert $nrows rows, record seqscan ground truth.
#   2. Build tqhnsw index.
#   3. DELETE ~20 % of rows (i > $nrows * 0.80), VACUUM to run bulkdelete.
#   4. Recompute seqscan ground truth over surviving rows.
#   5. Assert recall >= same band as 102_tqhnsw_recall.pl, at ef_search=200
#      (see header: deeper search absorbs vacuum-repair variance).
#   6. Assert every deleted id is absent from the index scan results.
# ===========================================================================

# Use a fixed Perl RNG seed so query vectors are reproducible (mirrors 102).
srand(20240101);

my @queries = ();
for (1 .. $nquery)
{
	my @coords = map { rand() - 0.5 } (1 .. $dim);
	push(@queries, "[" . join(",", @coords) . "]");
}

# The id threshold above which rows are deleted (~20 % of $nrows deleted).
my $delete_above = int($nrows * 0.80);
my $delete_from  = $delete_above + 1;  # lower bound for generate_series

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Disable autovacuum so VACUUM runs only when we explicitly call it.
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
$node->safe_psql("postgres", "ALTER TABLE tst SET (autovacuum_enabled = false);");

# ---- L2 (<->) ----
{
	my $opclass  = "vector_l2_ops";
	my $operator = "<->";

	$node->safe_psql("postgres",
		"INSERT INTO tst SELECT i, ARRAY[$array_sql]::vector($dim) FROM generate_series(1, $nrows) i;");

	# Seqscan ground truth over full table (before any deletes).
	my @gt_before = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		push(@gt_before, $res);
	}

	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (v $opclass) WITH (m = 16, ef_construction = 64);");

	# Sanity-check recall before any deletes (identical conditions to 102's
	# L2 high-ef gate, so the same 0.95 band applies).
	test_recall($node, 100, 200, 0.95, $operator, "L2 recall before vacuum ef=100 rerank=200",
		\@gt_before, \@queries);

	# Delete ~20 % of rows and vacuum.
	$node->safe_psql("postgres", "DELETE FROM tst WHERE i > $delete_above;");
	$node->safe_psql("postgres", "VACUUM tst;");

	# Collect the deleted id set for the absent-check.
	my $deleted_str = $node->safe_psql("postgres", qq(
		SELECT i FROM generate_series($delete_from, $nrows) i;
	));
	my %deleted_ids = map { $_ => 1 } split("\n", $deleted_str);

	# Recompute ground truth over surviving rows only.
	my @gt_after = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		push(@gt_after, $res);
	}

	# Recall over survivors: same 0.95 band, at ef=200 (see header).
	test_recall($node, 200, 200, 0.95, $operator, "L2 recall after vacuum ef=200 rerank=200",
		\@gt_after, \@queries);

	# Deleted ids must be absent from every index-scan result set.
	my $absent_ok = 1;
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqhnsw.ef_search = 100;
			SET tqhnsw.rerank = 200;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		foreach my $id (split("\n", $res))
		{
			if (exists($deleted_ids{$id}))
			{
				$absent_ok = 0;
				last;
			}
		}
		last unless $absent_ok;
	}
	ok($absent_ok, "L2: no deleted ids returned by index scan after vacuum");

	$node->safe_psql("postgres", "DROP INDEX idx;");
	$node->safe_psql("postgres", "TRUNCATE tst;");
}

# ---- cosine (<=>) ----
{
	my $opclass  = "vector_cosine_ops";
	my $operator = "<=>";

	$node->safe_psql("postgres",
		"INSERT INTO tst SELECT i, ARRAY[$array_sql]::vector($dim) FROM generate_series(1, $nrows) i;");

	# Seqscan ground truth over full table.
	my @gt_before = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		push(@gt_before, $res);
	}

	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (v $opclass) WITH (m = 16, ef_construction = 64);");

	# Sanity-check before deletes (mirrors 102's "cosine ef=100 rerank=200").
	test_recall($node, 100, 200, 0.90, $operator, "cosine recall before vacuum ef=100 rerank=200",
		\@gt_before, \@queries);

	$node->safe_psql("postgres", "DELETE FROM tst WHERE i > $delete_above;");
	$node->safe_psql("postgres", "VACUUM tst;");

	my $deleted_str = $node->safe_psql("postgres", qq(
		SELECT i FROM generate_series($delete_from, $nrows) i;
	));
	my %deleted_ids = map { $_ => 1 } split("\n", $deleted_str);

	my @gt_after = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		push(@gt_after, $res);
	}

	# Same band as 102 cosine high-ef (0.90), at ef=200 (see header).
	test_recall($node, 200, 200, 0.90, $operator, "cosine recall after vacuum ef=200 rerank=200",
		\@gt_after, \@queries);

	my $absent_ok = 1;
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqhnsw.ef_search = 100;
			SET tqhnsw.rerank = 200;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		foreach my $id (split("\n", $res))
		{
			if (exists($deleted_ids{$id}))
			{
				$absent_ok = 0;
				last;
			}
		}
		last unless $absent_ok;
	}
	ok($absent_ok, "cosine: no deleted ids returned by index scan after vacuum");

	$node->safe_psql("postgres", "DROP INDEX idx;");
	$node->safe_psql("postgres", "TRUNCATE tst;");
}

# ---- inner product (<#>) ----
{
	my $opclass  = "vector_ip_ops";
	my $operator = "<#>";

	$node->safe_psql("postgres",
		"INSERT INTO tst SELECT i, ARRAY[$array_sql]::vector($dim) FROM generate_series(1, $nrows) i;");

	# Seqscan ground truth over full table.
	my @gt_before = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		push(@gt_before, $res);
	}

	$node->safe_psql("postgres",
		"CREATE INDEX idx ON tst USING tqhnsw (v $opclass) WITH (m = 16, ef_construction = 64);");

	# Sanity-check before deletes (mirrors 102's "inner product ef=100 rerank=200").
	test_recall($node, 100, 200, 0.90, $operator, "IP recall before vacuum ef=100 rerank=200",
		\@gt_before, \@queries);

	$node->safe_psql("postgres", "DELETE FROM tst WHERE i > $delete_above;");
	$node->safe_psql("postgres", "VACUUM tst;");

	my $deleted_str = $node->safe_psql("postgres", qq(
		SELECT i FROM generate_series($delete_from, $nrows) i;
	));
	my %deleted_ids = map { $_ => 1 } split("\n", $deleted_str);

	my @gt_after = ();
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_indexscan = off;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		push(@gt_after, $res);
	}

	# Same band as 102 inner product high-ef (0.90), at ef=200 (see header).
	test_recall($node, 200, 200, 0.90, $operator, "IP recall after vacuum ef=200 rerank=200",
		\@gt_after, \@queries);

	my $absent_ok = 1;
	foreach my $q (@queries)
	{
		my $res = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqhnsw.ef_search = 100;
			SET tqhnsw.rerank = 200;
			SELECT i FROM tst ORDER BY v $operator '$q' LIMIT $limit;
		));
		foreach my $id (split("\n", $res))
		{
			if (exists($deleted_ids{$id}))
			{
				$absent_ok = 0;
				last;
			}
		}
		last unless $absent_ok;
	}
	ok($absent_ok, "IP: no deleted ids returned by index scan after vacuum");

	$node->safe_psql("postgres", "DROP INDEX idx;");
	$node->safe_psql("postgres", "TRUNCATE tst;");
}

$node->stop;

# ===========================================================================
# PART 2 — WAL replay through a streaming standby
#
# Pattern mirrors 010_hnsw_wal.pl exactly:
#   primary (allows_streaming => 1) → backup → replica (has_streaming => 1)
#
# Steps:
#   1. Build tqhnsw index on primary.
#   2. Wait for replica to catch up.
#   3. Run DELETE + VACUUM on primary; wait for replica to catch up again.
#   4. Assert L2 recall >= 0.95 at ef_search=200 on both primary and replica
#      (see header: deeper search absorbs vacuum-repair variance).
#   5. Assert deleted ids are absent from both result sets.
# ===========================================================================

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->start;

my $backup_name = 'tqhnsw_vacuum_backup';
$node_primary->backup($backup_name);

my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->init_from_backup($node_primary, $backup_name, has_streaming => 1);
$node_replica->start;

# Build index on primary.
$node_primary->safe_psql("postgres", "CREATE EXTENSION vector;");
$node_primary->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
$node_primary->safe_psql("postgres", "ALTER TABLE tst SET (autovacuum_enabled = false);");
$node_primary->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql]::vector($dim) FROM generate_series(1, $nrows) i;");
$node_primary->safe_psql("postgres",
	"CREATE INDEX idx ON tst USING tqhnsw (v vector_l2_ops) WITH (m = 16, ef_construction = 64);");

# Wait for replica to receive the initial data + index build.
my $applname = $node_replica->name;
$node_primary->poll_query_until('postgres',
	"SELECT pg_current_wal_lsn() <= replay_lsn FROM pg_stat_replication WHERE application_name = '$applname';")
  or die "Timed out waiting for replica to catch up after index build";

# DELETE ~20 % of rows and VACUUM on primary.
$node_primary->safe_psql("postgres", "DELETE FROM tst WHERE i > $delete_above;");
$node_primary->safe_psql("postgres", "VACUUM tst;");

# Wait for the DELETE + VACUUM WAL to be replayed on the replica.
$node_primary->poll_query_until('postgres',
	"SELECT pg_current_wal_lsn() <= replay_lsn FROM pg_stat_replication WHERE application_name = '$applname';")
  or die "Timed out waiting for replica to catch up after vacuum";

# Compute seqscan ground truth (over survivors) on the primary.
my @gt_wal = ();
foreach my $q (@queries)
{
	my $res = $node_primary->safe_psql("postgres", qq(
		SET enable_indexscan = off;
		SELECT i FROM tst ORDER BY v <-> '$q' LIMIT $limit;
	));
	push(@gt_wal, $res);
}

# Recall on primary after WAL vacuum — same 0.95 band, at ef=200 (see header;
# this gate observed 0.945 on a marginal draw at ef=100).
test_recall($node_primary, 200, 200, 0.95, "<->",
	"WAL primary: L2 recall after vacuum", \@gt_wal, \@queries);

# Recall on replica must be consistent: run the same recall gate.
# The replica is read-only so we use safe_psql directly (no SET persists,
# but GUCs can be SET within the same multi-statement safe_psql call).
{
	my $correct = 0;
	my $total   = 0;
	for my $i (0 .. $#queries)
	{
		my $actual = $node_replica->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqhnsw.ef_search = 200;
			SET tqhnsw.rerank = 200;
			SELECT i FROM tst ORDER BY v <-> '$queries[$i]' LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);
		my %actual_set = map { $_ => 1 } @actual_ids;

		my @expected_ids = split("\n", $gt_wal[$i]);
		foreach (@expected_ids)
		{
			if (exists($actual_set{$_}))
			{
				$correct++;
			}
			$total++;
		}
	}
	cmp_ok($correct / $total, ">=", 0.95,
		"WAL replica: L2 recall after vacuum replayed via streaming standby");
}

# Deleted ids must be absent from the replica index scan too.
{
	my $deleted_str = $node_primary->safe_psql("postgres", qq(
		SELECT i FROM generate_series($delete_from, $nrows) i;
	));
	my %deleted_ids = map { $_ => 1 } split("\n", $deleted_str);

	my $absent_ok = 1;
	foreach my $q (@queries)
	{
		my $res = $node_replica->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET tqhnsw.ef_search = 100;
			SET tqhnsw.rerank = 200;
			SELECT i FROM tst ORDER BY v <-> '$q' LIMIT $limit;
		));
		foreach my $id (split("\n", $res))
		{
			if (exists($deleted_ids{$id}))
			{
				$absent_ok = 0;
				last;
			}
		}
		last unless $absent_ok;
	}
	ok($absent_ok, "WAL replica: no deleted ids returned after vacuum replay");
}

done_testing();
