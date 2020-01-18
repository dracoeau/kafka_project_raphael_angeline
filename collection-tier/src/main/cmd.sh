
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic velib-nbfreedocks-count-notifications --replication-factor 1 --partitions 1 --config "cleanup.policy=compact" --config "delete.retention.ms=100" --config "segment.ms=100" --config "min.cleanable.dirty.ratio=0.01"

./bin/kafka-consumer-groups.sh -bootstrap-server localhost:9092 -describe -group group-test-1