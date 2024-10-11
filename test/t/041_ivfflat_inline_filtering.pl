use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node;
my @queries = ();
my @where = ();
my @expected;
my $limit = 20;
my $dim = 3;
my $array_sql = join(",", ('random()') x $dim);
my $nc = 100;
my $nc2 = 10;

sub test_recall
{
	my ($probes, $min, $operator) = @_;
	my $correct = 0;
	my $total = 0;

	for my $j (0 .. 2)
	{
		my $explain = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET ivfflat.probes = $probes;
			EXPLAIN ANALYZE SELECT i FROM tst WHERE $where[$j] ORDER BY v $operator '$queries[$j]' LIMIT $limit;
		));
		like($explain, qr/Index Cond/);
	}

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET ivfflat.probes = $probes;
			SELECT i FROM tst WHERE $where[$i] ORDER BY v $operator '$queries[$i]' LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);
		my %actual_set = map { $_ => 1 } @actual_ids;

		is(scalar(@actual_ids), $limit);

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
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim), c int4, c2 int4);");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql], i % $nc, i % $nc2 FROM generate_series(1, 50000) i;"
);

# Generate queries
for my $i (1 .. 100)
{
	my @r = ();
	for (1 .. $dim)
	{
		push(@r, rand());
	}
	push(@queries, "[" . join(",", @r) . "]");

	if ($i % 3 == 0)
	{
		my $c = int(rand() * $nc);
		push(@where, "c = $c");
	}
	elsif ($i % 3 == 1)
	{
		my $c2 = int(rand() * $nc2);
		push(@where, "c2 = $c2");
	}
	else
	{
		# use c2 to ensure results
		my $c2 = int(rand() * $nc2);
		push(@where, "c = $c2 AND c2 = $c2");
	}
}

# Add index
$node->safe_psql("postgres", qq(
	CREATE INDEX ON tst USING ivfflat (v vector_l2_ops, c, c2) WITH (lists = 100);
));

# Insert more rows
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql], i % $nc, i % $nc2 FROM generate_series(1, 50000) i;"
);

# Get exact results
@expected = ();
for my $i (0 .. $#queries)
{
	my $res = $node->safe_psql("postgres", qq(
		SET enable_indexscan = off;
		SELECT i FROM tst WHERE $where[$i] ORDER BY v <-> '$queries[$i]' LIMIT $limit;
	));
	push(@expected, $res);
}

# Test recall
test_recall(10, 0.99, '<->');

# Test vacuum
$node->safe_psql("postgres", "DELETE FROM tst WHERE c > 5;");
$node->safe_psql("postgres", "VACUUM tst;");

# Test less than
my $explain = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c < 10 ORDER BY v <-> '$queries[0]' LIMIT $limit;
));
like($explain, qr/Index Cond: \(c < 10\)/);

# Test less than or equal
$explain = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c <= 10 ORDER BY v <-> '$queries[0]' LIMIT $limit;
));
like($explain, qr/Index Cond: \(c <= 10\)/);

# Test greater than or equal
$explain = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c >= 90 ORDER BY v <-> '$queries[0]' LIMIT $limit;
));
like($explain, qr/Index Cond: \(c >= 90\)/);

# Test greater than
$explain = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c > 90 ORDER BY v <-> '$queries[0]' LIMIT $limit;
));
like($explain, qr/Index Cond: \(c > 90\)/);

# Test multiple attribute columns
$explain = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c = 1 AND c2 = 1 ORDER BY v <-> '$queries[0]' LIMIT $limit;
));
like($explain, qr/Index Cond: \(\(c = 1\) AND \(c2 = 1\)\)/);

# Test only last attribute column
$explain = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c2 = 1 ORDER BY v <-> '$queries[0]' LIMIT $limit;
));
like($explain, qr/Index Cond: \(c2 = 1\)/);

# Test only vector column
$explain = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v <-> '$queries[0]' LIMIT $limit;
));
like($explain, qr/Index Scan/);

# Test only attribute columns
$explain = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	EXPLAIN ANALYZE SELECT i FROM tst WHERE c = 1;
));
like($explain, qr/Seq Scan/);

# Test columns
my ($ret, $stdout, $stderr) = $node->psql("postgres", "CREATE INDEX ON tst USING ivfflat (c);");
like($stderr, qr/first column must be a vector/);

($ret, $stdout, $stderr) = $node->psql("postgres", "CREATE INDEX ON tst USING ivfflat (c, v vector_cosine_ops);");
like($stderr, qr/first column must be a vector/);

($ret, $stdout, $stderr) = $node->psql("postgres", "CREATE INDEX ON tst USING ivfflat (v vector_cosine_ops, c, c, c);");
like($stderr, qr/index cannot have more than three columns/);

($ret, $stdout, $stderr) = $node->psql("postgres", "CREATE INDEX ON tst USING ivfflat (v vector_cosine_ops, v vector_cosine_ops);");
like($stderr, qr/column 2 cannot be a vector/);

done_testing();
