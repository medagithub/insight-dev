~/dev/redis-stable/src/redis-cli flushall
~/dev/redis-stable/src/redis-cli keys "*"
~/dev/apache-cassandra-2.2.1/bin/cqlsh -f ~/dev/techklout/scripts/createCassandraSchema.cql
~/dev/apache-cassandra-2.2.1/bin/cqlsh -f ~/dev/techklout/scripts/cassandraSeedData.cql
