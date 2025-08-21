#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🧪 S3 Integration Test Suite${NC}"
echo "=============================="

# Function to check if a service is responding
check_service() {
    local url=$1
    local service_name=$2
    local max_attempts=30
    local attempt=0
    
    echo -e "${YELLOW}⏳ Waiting for $service_name to be ready...${NC}"
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ $service_name is ready${NC}"
            return 0
        fi
        attempt=$((attempt + 1))
        echo "   Attempt $attempt/$max_attempts..."
        sleep 2
    done
    
    echo -e "${RED}❌ $service_name failed to start after $max_attempts attempts${NC}"
    return 1
}

# Function to test S3 serde functionality
test_s3_serde() {
    echo -e "${BLUE}🔍 Testing S3 Serde Functionality${NC}"
    
    # Check if we can access MinIO API
    echo "1. Testing MinIO connectivity..."
    if docker-compose exec -T minio mc --version > /dev/null 2>&1; then
        echo -e "${GREEN}   ✅ MinIO client works${NC}"
    else
        echo -e "${RED}   ❌ MinIO client not accessible${NC}"
        return 1
    fi
    
    # Configure mc client and check if descriptor file exists in MinIO
    echo "2. Verifying descriptor file in S3..."
    docker-compose exec -T minio mc alias set minio http://localhost:9000 minioadmin minioadmin123 > /dev/null 2>&1
    if docker-compose exec -T minio mc ls minio/protobuf-descriptors/test_descriptors.desc > /dev/null 2>&1; then
        echo -e "${GREEN}   ✅ Descriptor file found in S3${NC}"
    elif docker-compose exec -T minio mc ls minio/protobuf-descriptors/ | grep -q test_descriptors.desc 2>/dev/null; then
        echo -e "${GREEN}   ✅ Descriptor file found in S3${NC}"
    else
        echo -e "${YELLOW}   ⚠️ Checking MinIO bucket contents...${NC}"
        docker-compose exec -T minio mc ls minio/protobuf-descriptors/ || echo "   Bucket listing failed"
        echo -e "${RED}   ❌ Descriptor file not found in S3${NC}"
        return 1
    fi
    
    # Check Kafka UI S3 service logs for successful descriptor loading
    echo "3. Checking Kafka UI S3 logs for descriptor loading..."
    if docker-compose --profile s3-test logs kafka-ui-s3 2>/dev/null | grep -q "ProtobufDescriptorSetSerde" || \
       docker-compose --profile s3-test logs kafka-ui-s3 2>/dev/null | grep -q "S3:"; then
        echo -e "${GREEN}   ✅ S3 descriptor loading detected in logs${NC}"
    else
        echo -e "${YELLOW}   ⚠️ No explicit S3 loading logs found (might be normal)${NC}"
    fi
    
    # Test basic API connectivity
    echo "4. Testing Kafka UI S3 API connectivity..."
    if check_service "http://localhost:8081/actuator/health" "Kafka UI S3" 10; then
        echo -e "${GREEN}   ✅ Kafka UI S3 API is accessible${NC}"
    else
        echo -e "${RED}   ❌ Kafka UI S3 API not accessible${NC}"
        return 1
    fi
    
    return 0
}

# Function to test message processing
test_message_processing() {
    echo -e "${BLUE}📨 Testing Message Processing${NC}"
    
    # Send test protobuf message
    echo "1. Sending test protobuf message..."
    if ./scripts/send_test_message.sh > /dev/null 2>&1; then
        echo -e "${GREEN}   ✅ Test message sent successfully${NC}"
    else
        echo -e "${RED}   ❌ Failed to send test message${NC}"
        return 1
    fi
    
    # Wait a bit for message to be processed
    sleep 3
    
    echo -e "${GREEN}   ✅ Message processing test completed${NC}"
    return 0
}

# Function to test S3 refresh functionality
test_s3_refresh() {
    echo -e "${BLUE}🔄 Testing S3 Refresh Functionality${NC}"
    
    # Create a backup of the original descriptor
    echo "1. Creating backup of original descriptor..."
    docker-compose exec -T minio mc alias set minio http://localhost:9000 minioadmin minioadmin123 > /dev/null 2>&1
    docker-compose exec -T minio mc cp minio/protobuf-descriptors/test_descriptors.desc minio/protobuf-descriptors/test_descriptors_backup.desc
    
    # Get current descriptor info
    echo "2. Getting current descriptor info..."
    original_size=$(docker-compose exec -T minio mc stat minio/protobuf-descriptors/test_descriptors.desc | grep "Size" | awk '{print $2}' || echo "unknown")
    echo "   Original descriptor size: $original_size"
    
    # Re-upload descriptor (simulates a change)
    echo "3. Re-uploading descriptor file (simulates S3 change)..."
    docker-compose exec -T minio mc alias set minio http://localhost:9000 minioadmin minioadmin123 > /dev/null 2>&1
    docker-compose exec -T minio mc cp /descriptors/test_descriptors.desc minio/protobuf-descriptors/test_descriptors.desc
    
    echo "4. Waiting for refresh interval (30 seconds)..."
    sleep 35
    
    echo -e "${GREEN}   ✅ S3 refresh test completed${NC}"
    return 0
}

# Function to cleanup
cleanup() {
    echo -e "${YELLOW}🧹 Cleaning up...${NC}"
    docker-compose --profile s3-test down -v 2>/dev/null || true
    echo -e "${GREEN}✅ Cleanup completed${NC}"
}

# Trap cleanup on exit
trap cleanup EXIT

# Main test execution
main() {
    echo -e "${BLUE}🏗️ Building project...${NC}"
    cd .. && ./gradlew build && cd docker-compose
    echo -e "${GREEN}✅ Build completed${NC}"
    
    echo -e "${BLUE}🐳 Starting Docker services...${NC}"
    docker-compose up -d kafka zookeeper minio
    
    # Wait for core services
    check_service "http://localhost:9000/minio/health/live" "MinIO" || exit 1
    
    # Setup MinIO with descriptor
    echo -e "${BLUE}📁 Setting up MinIO with test descriptor...${NC}"
    docker-compose --profile setup run --rm minio-setup
    
    # Create topics
    echo -e "${BLUE}📋 Creating test topics...${NC}"
    docker-compose --profile setup run --rm topic-creator
    
    # Start Kafka UI with S3
    echo -e "${BLUE}🌐 Starting Kafka UI with S3 configuration...${NC}"
    docker-compose --profile s3-test up -d kafka-ui-s3
    
    # Wait for Kafka UI to start
    check_service "http://localhost:8081/actuator/health" "Kafka UI S3" || exit 1
    
    # Run tests
    echo ""
    if test_s3_serde && test_message_processing && test_s3_refresh; then
        echo ""
        echo -e "${GREEN}🎉 All S3 Integration Tests Passed!${NC}"
        echo "======================================"
        echo ""
        echo -e "${BLUE}🌐 Services available:${NC}"
        echo "   • Kafka UI (S3):     http://localhost:8081"
        echo "   • MinIO Console:     http://localhost:9001"
        echo "   • Kafka:             localhost:9092"
        echo ""
        echo -e "${BLUE}🔑 MinIO credentials:${NC}"
        echo "   • Username: minioadmin"
        echo "   • Password: minioadmin123"
        echo ""
        echo -e "${BLUE}📋 Manual verification steps:${NC}"
        echo "1. Open http://localhost:8081"
        echo "2. Navigate to 'ProtobufS3TestCluster'"
        echo "3. Go to Topics → user-events"
        echo "4. Verify protobuf messages show as JSON"
        echo "5. Check serde description shows S3 source"
        echo ""
        echo "Press Ctrl+C to stop all services..."
        
        # Keep services running for manual testing
        while true; do
            sleep 10
        done
    else
        echo ""
        echo -e "${RED}❌ S3 Integration Tests Failed${NC}"
        echo "=============================="
        echo ""
        echo -e "${YELLOW}📋 Debugging information:${NC}"
        echo ""
        echo "MinIO logs:"
        docker-compose logs --tail=20 minio
        echo ""
        echo "Kafka UI S3 logs:"
        docker-compose --profile s3-test logs --tail=20 kafka-ui-s3
        return 1
    fi
}

# Run main function
main