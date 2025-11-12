#!/bin/bash

# Start S3 Topic Mapping Integration Test Environment
# Quick setup for testing S3-based topic mapping configuration

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting S3 Topic Mapping Test Environment${NC}"
echo "=============================================="

# Function to check service health
check_service() {
    local url=$1
    local service_name=$2
    local max_attempts=30
    local attempt=1
    
    echo -n "Checking ${service_name}..."
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s "$url" > /dev/null 2>&1; then
            echo -e " ${GREEN}✓ Ready${NC}"
            return 0
        fi
        printf "."
        sleep 2
        ((attempt++))
    done
    
    echo -e " ${RED}✗ Failed after ${max_attempts} attempts${NC}"
    return 1
}

# Start the core services first (kafka, zookeeper, minio)
echo -e "${BLUE}🐳 Starting core services (Kafka, Zookeeper, MinIO)...${NC}"
docker-compose up -d kafka zookeeper minio

# Wait for services to be healthy
echo -e "${BLUE}⏳ Waiting for core services to be ready...${NC}"
echo -n "Waiting for services to be healthy..."

# Wait for all services to be healthy (docker-compose health checks)
while ! docker-compose ps kafka | grep -q "healthy" || \
      ! docker-compose ps zookeeper | grep -q "Up" || \
      ! docker-compose ps minio | grep -q "healthy"; do
    sleep 2
    printf "."
done
echo -e " ${GREEN}✓ All services ready${NC}"

# Verify connectivity
check_service "http://localhost:9000/minio/health/live" "MinIO" || exit 1

# Setup MinIO with test data
echo -e "${BLUE}📁 Setting up MinIO with test files...${NC}"
docker-compose --profile setup run --rm minio-setup

# Verify descriptor and topic mappings files in MinIO
echo -e "${BLUE}🔍 Verifying files in MinIO...${NC}"
docker-compose exec -T minio mc alias set minio http://localhost:9000 minioadmin minioadmin123 > /dev/null 2>&1
if docker-compose exec -T minio mc ls minio/protobuf-descriptors/ | grep -q test_descriptors.desc && \
   docker-compose exec -T minio mc ls minio/protobuf-descriptors/ | grep -q topic-mappings.json; then
    echo -e "${GREEN}✅ Both descriptor and topic mappings files successfully uploaded to MinIO${NC}"
    echo -e "${BLUE}📋 MinIO bucket contents:${NC}"
    docker-compose exec -T minio mc ls minio/protobuf-descriptors/
    
    # Show topic mappings content
    echo -e "${BLUE}📄 Topic mappings content:${NC}"
    docker-compose exec -T minio mc cat minio/protobuf-descriptors/topic-mappings.json | jq . || echo "   (JSON formatting failed, raw content shown above)"
else
    echo -e "${RED}❌ Failed to verify files in MinIO${NC}"
    echo -e "${BLUE}📋 Checking MinIO bucket contents:${NC}"
    docker-compose exec -T minio mc ls minio/protobuf-descriptors/ || echo "   Bucket listing failed"
    exit 1
fi

# Build the serde JAR if it doesn't exist
if [ ! -f "../build/libs/kafbat-ui-serde-protobuf-descriptor-0.1.5.jar" ]; then
    echo -e "${BLUE}🔨 Building serde JAR file...${NC}"
    cd ..
    ./gradlew build -x test
    cd docker-compose
fi

# Start Kafka UI with S3 topic mapping configuration
echo -e "${BLUE}🌐 Starting Kafka UI with S3 topic mapping configuration...${NC}"
docker-compose --profile s3-topic-mapping-test up -d kafka-ui-s3-topic-mapping

# Wait for Kafka UI to be ready
echo -e "${BLUE}⏳ Waiting for Kafka UI S3 topic mapping service...${NC}"
check_service "http://localhost:8082/actuator/health" "Kafka UI S3 Topic Mapping" || exit 1

# Create test topics
echo -e "${BLUE}📝 Creating test topics...${NC}"
topics=("user-events" "order-events" "payment-events" "notification-events")
for topic in "${topics[@]}"; do
    if ! docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list | grep -q "^${topic}$"; then
        echo "   Creating topic: $topic"
        docker-compose exec -T kafka kafka-topics --create --bootstrap-server localhost:29092 --topic "$topic" --partitions 1 --replication-factor 1 > /dev/null 2>&1
    else
        echo "   Topic already exists: $topic"
    fi
done

# Success message
echo
echo -e "${GREEN}🎉 S3 Topic Mapping Test Environment Started Successfully!${NC}"
echo
echo -e "${BLUE}🔗 Access URLs:${NC}"
echo "   • Kafka UI (S3 Topic Mapping): http://localhost:8082"
echo "   • MinIO Console:               http://localhost:9001"
echo
echo -e "${BLUE}🔑 MinIO credentials:${NC}"
echo "   • Username: minioadmin"
echo "   • Password: minioadmin123"
echo
echo -e "${BLUE}📊 Configuration Details:${NC}"
echo "   • Descriptors loaded from:     s3://protobuf-descriptors/test_descriptors.desc"
echo "   • Topic mappings loaded from:  s3://protobuf-descriptors/topic-mappings.json"
echo "   • Local override for:          payment-events → test.Order"
echo "   • S3 mappings for:             user-events → test.User, order-events → test.Order"
echo
echo -e "${BLUE}🧪 Testing Instructions:${NC}"
echo "1. Open Kafka UI at http://localhost:8082"
echo "2. Go to 'Topics' and select any of: user-events, order-events, payment-events"
echo "3. Check the serde description shows both descriptor and topic mapping sources"
echo "4. Produce/consume messages to verify topic-specific message type handling"
echo "5. Verify that payment-events uses 'test.Order' (local override)"
echo "6. Verify that user-events uses 'test.User' (from S3)"
echo
echo -e "${BLUE}📋 Available Topics:${NC}"
for topic in "${topics[@]}"; do
    echo "   • $topic"
done
echo
echo -e "${BLUE}🧹 To stop everything:${NC}"
echo "   docker-compose --profile s3-topic-mapping-test down -v"
echo
echo -e "${BLUE}📝 To run full integration test:${NC}"
echo "   ./test-s3-topic-mapping-integration.sh"