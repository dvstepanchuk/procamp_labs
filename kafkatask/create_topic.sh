cd /usr/lib/kafka/bin
kafka-topics.sh --zookeeper localhost:2181 --topic btcusd-transaction-topic --create --partitions 3 --replication-factor 3
