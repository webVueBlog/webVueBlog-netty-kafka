
Http (Netty) producer for Apache Kafka

Start Zookeeper

```
./bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka

```
./bin/kafka-server-start.sh config/server.properties
```

Create topic

```
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic example
```

