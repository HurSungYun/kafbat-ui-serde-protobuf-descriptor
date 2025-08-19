# Integration Testing with Docker Compose

This directory contains a complete Docker Compose setup for integration testing the Protobuf Descriptor Set Serde with kafbat/kafka-ui.

## Overview

The integration test setup includes:
- **kafbat UI**: Web interface for Kafka management with the protobuf serde configured
- **Apache Kafka**: Message broker for testing
- **Zookeeper**: Kafka coordination service
- **Message Producer**: Python service that generates sample protobuf messages

## Prerequisites

- Docker and Docker Compose installed
- Built serde JAR file (`make build` to create it)

## Quick Start

1. **Build the serde JAR**:
   ```bash
   cd .. && make build
   ```

2. **Start the services**:
   ```bash
   docker-compose up -d
   ```

3. **Access kafbat UI**:
   Open http://localhost:8080 in your browser

4. **Produce test messages** (optional):
   ```bash
   docker-compose --profile producer up -d producer
   ```

## Services

### kafbat UI (Port 8080)
- **Image**: `ghcr.io/kafbat/kafka-ui:latest`
- **Configuration**: 
  - Cluster: `ProtobufTestCluster`
  - Serde: `ProtobufDescriptorSetSerde`
  - Descriptor file: `/descriptors/test_descriptors.desc`

### Kafka (Port 9092)
- **Image**: `confluentinc/cp-kafka:7.4.0`
- **Configuration**: Single broker setup with auto-topic creation enabled

### Zookeeper (Port 2181)
- **Image**: `confluentinc/cp-zookeeper:7.4.0` 
- **Purpose**: Kafka coordination and metadata storage

### Message Producer (Optional)
- **Custom Image**: Python-based protobuf message generator
- **Topics**: `user-events`, `order-events`
- **Message Types**: User and Order protobuf messages

## Test Scenarios

### 1. Basic Functionality Test
1. Start the services: `docker-compose up -d`
2. Access kafbat UI at http://localhost:8080
3. Verify the cluster appears and serde is loaded
4. Check Topics section (should show auto-created topics)

### 2. Message Deserialization Test
1. Start producer: `docker-compose --profile producer up -d producer`
2. Wait for messages to be produced (check logs: `docker-compose logs producer`)
3. In kafbat UI, navigate to Topics > `user-events` or `order-events`
4. View messages - they should be displayed as formatted JSON instead of raw bytes
5. Verify message metadata shows protobuf message type information

### 3. Serde Configuration Test
1. Check kafbat UI configuration in Overview section
2. Verify `ProtobufDescriptorSetSerde` appears in configured serdes
3. Confirm descriptor file path is correctly mounted

## Test Data

The test setup uses the following protobuf messages:

### User Messages
```protobuf
message User {
  int32 id = 1;
  string name = 2;
  string email = 3;
  repeated string tags = 4;
  UserType type = 5;
  Address address = 6;
}
```

### Order Messages  
```protobuf
message Order {
  int64 id = 1;
  User user = 2;
  repeated OrderItem items = 3;
  double total_amount = 4;
  OrderStatus status = 5;
  int64 created_timestamp = 6;
}
```

## Troubleshooting

### Serde Not Loading
- Check that the JAR file exists: `ls -la ../build/libs/`
- Verify JAR is mounted correctly: `docker-compose exec kafka-ui ls -la /serde/`
- Check kafbat UI logs: `docker-compose logs kafka-ui`

### Messages Not Deserializing
- Ensure descriptor file is mounted: `docker-compose exec kafka-ui ls -la /descriptors/`
- Check producer logs: `docker-compose logs producer` 
- Verify topics have messages: Use kafbat UI or run `docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic user-events --from-beginning`

### Connection Issues
- Wait for all services to be healthy: `docker-compose ps`
- Check service logs: `docker-compose logs [service-name]`
- Ensure ports are not in use: `netstat -an | grep 8080`

## Commands

```bash
# Start all services
docker-compose up -d

# Start with message producer
docker-compose --profile producer up -d

# View logs
docker-compose logs -f [service-name]

# Check service status
docker-compose ps

# Stop all services  
docker-compose down

# Clean up everything (including volumes)
docker-compose down -v --remove-orphans

# Rebuild producer image
docker-compose build producer

# Manual message production
docker-compose run --rm producer python /scripts/produce_messages.py
```

## Expected Results

When working correctly, you should see:

1. **kafbat UI Dashboard**: Shows `ProtobufTestCluster` with connected broker
2. **Topics**: `user-events` and `order-events` topics created by producer  
3. **Messages**: Protobuf messages displayed as formatted JSON with proper field names
4. **Metadata**: Message type information shown (e.g., "test.User", "test.Order")
5. **Deserialization**: No hex strings or raw bytes visible for protobuf messages

## Configuration Details

The serde is configured with these environment variables:
- `KAFKA_CLUSTERS_0_SERDE_0_PROPERTIES_PROTOBUF_DESCRIPTOR_SET_FILE=/descriptors/test_descriptors.desc`

This points to the descriptor set file containing definitions for:
- `user.proto`: User, Address messages and UserType enum
- `order.proto`: Order, OrderItem messages and OrderStatus enum (imports user.proto)

## Files Structure

```
docker/
├── docker-compose.yml          # Main compose file
├── Dockerfile.producer         # Producer image definition  
├── README.md                   # This file
├── descriptors/
│   └── test_descriptors.desc   # Protobuf descriptor set
└── scripts/
    └── produce_messages.py     # Message producer script
```