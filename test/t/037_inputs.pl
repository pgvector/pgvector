use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create extension
$node->safe_psql("postgres", "CREATE EXTENSION vector;");

my @types = ("vector", "halfvec", "sparsevec");
my @inputs = ("[1.23,4.56,7.89]", "[1.23,4.56,7.89]", "{1:1.23,2:4.56,3:7.89}/3");
my @subs = (" ", " ", ",", ":", "-", "1", "9", "\0", "2147483648", "-2147483649");

for my $i (0 .. $#types)
{
	my $type = $types[$i];

	for (1 .. 100)
	{
		my $input = $inputs[$i] . "";

		for (1 .. 1 + int(rand() * 2))
		{
			my $r = int(rand() * length($input));
			my $sub = $subs[int(rand() * scalar(@subs))];
			if ($sub eq "\0")
			{
				# Truncate
				$input = substr($input, 0, $r);
			}
			elsif (rand() > 0.5)
			{
				# Insert
				substr($input, $r, 0) = $sub;
			}
			else
			{
				# Replace
				substr($input, $r, length($sub), $sub);
			}
		}

		my ($ret, $stdout, $stderr) = $node->psql("postgres", "SELECT '$input'::$type;");
		if ($ret != 0)
		{
			# Test for type in error message
			like($stderr, qr/$type/);
		}
		else
		{
			# Count test
			is($ret, 0);
		}
	}
}

done_testing();
