package PostgreSQL::Test::Cluster;

use PostgresNode;

sub new
{
	my ($class, $name) = @_;
	return get_new_node($name);
}

1;
