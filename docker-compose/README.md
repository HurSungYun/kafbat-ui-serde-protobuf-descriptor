# Integration Testing with Docker Compose

This directory contains a complete Docker Compose setup for integration testing the Protobuf Descriptor Set Serde with kafbat/kafka-ui.

## Overview

The integration test setup includes:
- **kafbat UI**: Web interface for Kafka management with the protobuf serde configured
- **Apache Kafka**: Message broker for testing
- **Zookeeper**: Kafka coordination service
- **MinIO**: S3-compatible storage for testing S3 descriptor sources
- **Message Producer**: Python service that generates sample protobuf messages

## Test Modes

### 1. Local File Mode (Original)
Tests descriptor loading from local files mounted as volumes.

### 2. S3 Mode (New)
Tests descriptor loading from MinIO S3-compatible storage with caching and refresh functionality.

## Prerequisites

- Docker and Docker Compose installed
- Built serde JAR file (`make build` to create it)

## Quick Start

### Local File Mode (Original)

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

### S3 Mode (MinIO)

1. **Run S3 integration test**:
   ```bash
   cd .. && make s3-test
   ```
   
   Or use the interactive mode:
   ```bash
   cd .. && make s3-test-start
   ```

2. **Access services**:
   - Kafka UI (S3): http://localhost:8081
   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)

### S3 Topic Mapping Mode (MinIO)

Tests S3-based topic mapping configuration with local property overrides.

1. **Run S3 topic mapping integration test**:
   ```bash
   ./test-s3-topic-mapping-integration.sh
   ```
   
   Or start the environment interactively:
   ```bash
   ./start-s3-topic-mapping-test.sh
   ```

2. **Access services**:
   - Kafka UI (S3 Topic Mapping): http://localhost:8082
   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)

3. **Configuration tested**:
   - **S3 topic mappings**: Loaded from `s3://protobuf-descriptors/topic-mappings.json`
   - **Local overrides**: `payment-events` → `test.Order` (overrides S3)
   - **S3 mappings**: `user-events` → `test.User`, `order-events` → `test.Order`

### Quick Commands via Makefile

From the project root:
```bash
# Local file integration test
make integration-test

# S3 integration test (automated)
make s3-test

# S3 integration test (interactive)
make s3-test-start

# Run both tests
make integration-test-full

# View S3 test logs
make s3-test-logs

# Stop S3 test
make s3-test-stop
```

## Services

### Kafka UI - Local Mode (Port 8080)
- **Image**: `ghcr.io/kafbat/kafka-ui:latest`
- **Configuration**: 
  - Cluster: `ProtobufTestCluster`
  - Serde: `ProtobufDescriptorSetSerde`
  - Descriptor source: Local file `/descriptors/test_descriptors.desc`

### Kafka UI - S3 Mode (Port 8081) 
- **Image**: `ghcr.io/kafbat/kafka-ui:latest`
- **Configuration**: 
  - Cluster: `ProtobufS3TestCluster`
  - Serde: `ProtobufDescriptorSetSerdeS3`
  - Descriptor source: S3 `s3://protobuf-descriptors/test_descriptors.desc`
  - Refresh interval: 30 seconds
- **Profile**: `s3-test`

### MinIO (Ports 9000, 9001)
- **Image**: `minio/minio:latest`
- **Purpose**: S3-compatible storage for descriptor files
- **Console**: http://localhost:9001 (minioadmin/minioadmin123)
- **Bucket**: `protobuf-descriptors`

### Kafka (Port 9092)
- **Image**: `confluentinc/cp-kafka:7.4.0`
- **Configuration**: Single broker setup with auto-topic creation enabled

### Zookeeper (Port 2181)
- **Image**: `confluentinc/cp-zookeeper:7.4.0` 
- **Purpose**: Kafka coordination and metadata storage

### MinIO Setup Service
- **Image**: `minio/mc:latest`
- **Purpose**: Creates bucket and uploads test descriptor
- **Profile**: `setup`

### Topic Creator Service
- **Image**: `confluentinc/cp-kafka:7.4.0`
- **Purpose**: Creates test topics
- **Profile**: `setup`

## Test Scenarios

### 1. Local File Mode Tests

#### Basic Functionality Test
1. Start the services: `docker-compose up -d`
2. Access kafbat UI at http://localhost:8080
3. Verify the cluster appears and serde is loaded
4. Check Topics section (should show auto-created topics)

#### Message Deserialization Test
1. Send test messages: `./scripts/send_test_message.sh`
2. In kafbat UI, navigate to Topics > `user-events` or `order-events`
3. View messages - they should be displayed as formatted JSON instead of raw bytes
4. Verify message metadata shows protobuf message type information

### 2. S3 Mode Tests

#### Automated S3 Test
```bash
# From project root
make s3-test
```
This runs a comprehensive automated test that:
- Builds the project
- Starts MinIO and Kafka
- Uploads descriptor to S3
- Starts Kafka UI with S3 configuration
- Validates S3 connectivity and descriptor loading
- Tests refresh functionality

#### Interactive S3 Test
```bash
# From project root  
make s3-test-start
```
This starts the services and provides manual testing instructions.

#### S3 Refresh Test
1. Access MinIO console: http://localhost:9001 (minioadmin/minioadmin123)
2. Navigate to `protobuf-descriptors` bucket
3. Upload a modified descriptor file
4. Wait 30 seconds (refresh interval)
5. Verify Kafka UI picks up changes without restart

### 3. Comparison Test
Run both modes simultaneously to compare:
```bash
# Start local mode
docker-compose up -d kafka-ui

# Start S3 mode  
docker-compose --profile s3-test up -d kafka-ui-s3

# Access both:
# Local:  http://localhost:8080
# S3:     http://localhost:8081
```

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