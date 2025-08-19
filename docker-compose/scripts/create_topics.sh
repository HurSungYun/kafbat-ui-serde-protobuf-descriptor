#!/bin/bash

set -e

echo "Creating test topics for protobuf messages..."

KAFKA_BROKERS=${KAFKA_BROKERS:-kafka:29092}

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

# Create topics
echo "Creating topics..."
kafka-topics --bootstrap-server $KAFKA_BROKERS --create --topic user-events --partitions 3 --replication-factor 1 --if-not-exists
kafka-topics --bootstrap-server $KAFKA_BROKERS --create --topic order-events --partitions 3 --replication-factor 1 --if-not-exists
kafka-topics --bootstrap-server $KAFKA_BROKERS --create --topic test-protobuf-data --partitions 1 --replication-factor 1 --if-not-exists

echo "Topics created successfully!"
echo "Available topics:"
kafka-topics --bootstrap-server $KAFKA_BROKERS --list

echo "Topic descriptions:"
kafka-topics --bootstrap-server $KAFKA_BROKERS --describe --topic user-events
kafka-topics --bootstrap-server $KAFKA_BROKERS --describe --topic order-events
kafka-topics --bootstrap-server $KAFKA_BROKERS --describe --topic test-protobuf-data