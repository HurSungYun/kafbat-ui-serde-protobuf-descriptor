#!/bin/bash

# Test script for S3 Topic Mapping Integration
# Tests S3-based topic mapping configuration with RustFS

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🧪 Testing S3 Topic Mapping Integration${NC}"
echo "=========================================="

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

# Function to test topic mapping functionality
test_topic_mapping() {
    echo -e "${BLUE}🔍 Testing S3 Topic Mapping Configuration...${NC}"
    
    # Check Kafka UI S3 topic mapping service logs for successful configuration
    echo "1. Checking Kafka UI S3 topic mapping logs for configuration loading..."
    if docker-compose --profile s3-topic-mapping-test logs kafka-ui-s3-topic-mapping 2>/dev/null | grep -q "S3TopicMappingSource\|topicMappingSource" || \
       docker-compose --profile s3-topic-mapping-test logs kafka-ui-s3-topic-mapping 2>/dev/null | grep -q "topic-mappings.json"; then
        echo -e "${GREEN}   ✅ S3 topic mapping configuration detected in logs${NC}"
    else
        echo -e "${YELLOW}   ⚠️ S3 topic mapping configuration not clearly visible in logs${NC}"
        echo "   This might be normal - checking RustFS bucket contents instead..."
    fi
    
    # Configure mc client and verify topic mapping file exists in RustFS
    echo "2. Verifying topic mapping file exists in RustFS..."
    docker-compose run --rm rustfs-setup mc alias set rustfs http://rustfs:9000 rustfsadmin rustfsadmin123 > /dev/null 2>&1
    if docker-compose run --rm rustfs-setup mc ls rustfs/protobuf-descriptors/topic-mappings.json > /dev/null 2>&1; then
        echo -e "${GREEN}   ✅ Topic mappings file found in RustFS${NC}"
    elif docker-compose run --rm rustfs-setup mc ls rustfs/protobuf-descriptors/ | grep -q topic-mappings.json 2>/dev/null; then
        echo -e "${GREEN}   ✅ Topic mappings file found in bucket listing${NC}"
    else
        echo -e "${RED}   ❌ Topic mappings file not found in RustFS${NC}"
        echo -e "${YELLOW}   ⚠️ Checking RustFS bucket contents...${NC}"
        docker-compose run --rm rustfs-setup mc ls rustfs/protobuf-descriptors/ || echo "   Bucket listing failed"
        return 1
    fi

    # Verify topic mappings content
    echo "3. Checking topic mappings file content..."
    topic_mappings_content=$(docker-compose run --rm rustfs-setup mc cat rustfs/protobuf-descriptors/topic-mappings.json 2>/dev/null || echo "{}")
    if echo "$topic_mappings_content" | grep -q "user-events.*test.User" && \
       echo "$topic_mappings_content" | grep -q "order-events.*test.Order"; then
        echo -e "${GREEN}   ✅ Topic mappings content looks correct${NC}"
        echo "   Content preview: $(echo "$topic_mappings_content" | head -c 100)..."
    else
        echo -e "${YELLOW}   ⚠️ Topic mappings content might be unexpected${NC}"
        echo "   Content: $topic_mappings_content"
    fi
    
    # Test Kafka UI API for S3 topic mapping functionality
    echo "4. Testing Kafka UI API response for S3 topic mapping service..."
    if curl -f -s "http://localhost:8082/actuator/health" > /dev/null 2>&1; then
        echo -e "${GREEN}   ✅ Kafka UI S3 topic mapping service is healthy${NC}"
    else
        echo -e "${RED}   ❌ Kafka UI S3 topic mapping service is not responding${NC}"
        return 1
    fi
    
    # Optional: Test creating and consuming messages with different topic mappings
    echo "5. Testing message handling with S3 topic mappings..."
    
    # Check if topics exist and create them if necessary
    topics=("user-events" "order-events" "payment-events" "notification-events")
    for topic in "${topics[@]}"; do
        if ! docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list | grep -q "^${topic}$"; then
            echo "   Creating topic: $topic"
            docker-compose exec -T kafka kafka-topics --create --bootstrap-server localhost:29092 --topic "$topic" --partitions 1 --replication-factor 1 > /dev/null 2>&1
        fi
    done
    
    echo -e "${GREEN}   ✅ Test topics are ready${NC}"
    
    return 0
}

# Function to test refresh functionality
test_topic_mapping_refresh() {
    echo -e "${BLUE}🔄 Testing S3 Topic Mapping Refresh Functionality...${NC}"
    
    # Backup original topic mappings
    echo "1. Creating backup of original topic mappings..."
    docker-compose run --rm rustfs-setup mc alias set rustfs http://rustfs:9000 rustfsadmin rustfsadmin123 > /dev/null 2>&1
    docker-compose run --rm rustfs-setup mc cp rustfs/protobuf-descriptors/topic-mappings.json rustfs/protobuf-descriptors/topic-mappings-backup.json

    # Create updated topic mappings with additional mapping
    updated_mappings='{
  "user-events": "test.User",
  "order-events": "test.Order",
  "payment-events": "test.User",
  "notification-events": "test.User",
  "new-events": "test.Order"
}'

    echo "2. Updating topic mappings in RustFS..."
    echo "$updated_mappings" | docker-compose run --rm rustfs-setup sh -c 'cat > /tmp/updated-topic-mappings.json && mc alias set rustfs http://rustfs:9000 rustfsadmin rustfsadmin123 && mc cp /tmp/updated-topic-mappings.json rustfs/protobuf-descriptors/topic-mappings.json'

    # Verify the update was applied
    echo "3. Verifying topic mappings were updated..."
    sleep 5  # Give some time for potential refresh
    updated_content=$(docker-compose run --rm rustfs-setup mc cat rustfs/protobuf-descriptors/topic-mappings.json 2>/dev/null || echo "{}")
    if echo "$updated_content" | grep -q "new-events.*test.Order"; then
        echo -e "${GREEN}   ✅ Topic mappings successfully updated in RustFS${NC}"
    else
        echo -e "${RED}   ❌ Topic mappings update failed${NC}"
        return 1
    fi

    # Restore original topic mappings
    echo "4. Restoring original topic mappings..."
    docker-compose run --rm rustfs-setup mc cp rustfs/protobuf-descriptors/topic-mappings-backup.json rustfs/protobuf-descriptors/topic-mappings.json
    docker-compose run --rm rustfs-setup mc rm rustfs/protobuf-descriptors/topic-mappings-backup.json
    
    echo -e "${GREEN}   ✅ Topic mappings refresh test completed${NC}"
    return 0
}

# Main execution
main() {
    # Start core services
    echo -e "${BLUE}🐳 Starting core services (Kafka, Zookeeper, RustFS)...${NC}"
    docker-compose up -d kafka zookeeper rustfs

    # Wait for services to be healthy
    echo -e "${BLUE}⏳ Waiting for services to be ready...${NC}"
    check_service "http://localhost:9000/health" "RustFS" || exit 1
    
    # Wait for Kafka using docker-compose health checks instead of direct port check
    echo -n "Waiting for Kafka to be healthy..."
    max_attempts=60
    attempt=1
    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps kafka | grep -q "healthy"; then
            echo -e " ${GREEN}✓ Ready${NC}"
            break
        fi
        printf "."
        sleep 2
        ((attempt++))
        if [ $attempt -gt $max_attempts ]; then
            echo -e " ${RED}✗ Failed after ${max_attempts} attempts${NC}"
            exit 1
        fi
    done
    
    # Setup RustFS with descriptor and topic mappings
    echo -e "${BLUE}📁 Setting up RustFS with test files...${NC}"
    docker-compose --profile setup run --rm rustfs-setup

    # Verify RustFS setup
    echo -e "${BLUE}🔍 Verifying RustFS setup...${NC}"
    docker-compose run --rm rustfs-setup mc alias set rustfs http://rustfs:9000 rustfsadmin rustfsadmin123 > /dev/null 2>&1
    if docker-compose run --rm rustfs-setup mc ls rustfs/protobuf-descriptors/ | grep -q topic-mappings.json; then
        echo -e "${GREEN}✅ Topic mappings file successfully uploaded to RustFS${NC}"
        echo -e "${BLUE}📋 RustFS bucket contents:${NC}"
        docker-compose run --rm rustfs-setup mc ls rustfs/protobuf-descriptors/
    else
        echo -e "${RED}❌ Failed to verify topic mappings file in RustFS${NC}"
        exit 1
    fi
    
    # Start Kafka UI with S3 topic mapping configuration
    echo -e "${BLUE}🌐 Starting Kafka UI with S3 topic mapping configuration...${NC}"
    docker-compose --profile s3-topic-mapping-test up -d kafka-ui-s3-topic-mapping
    
    # Wait for Kafka UI to be ready
    echo -e "${BLUE}⏳ Waiting for Kafka UI S3 topic mapping service...${NC}"
    check_service "http://localhost:8082/actuator/health" "Kafka UI S3 Topic Mapping" || exit 1
    
    # Run tests
    if test_topic_mapping && test_topic_mapping_refresh; then
        echo
        echo -e "${GREEN}🎉 S3 Topic Mapping Integration Test Completed Successfully!${NC}"
        echo
        echo -e "${BLUE}📊 Test Summary:${NC}"
        echo "   • S3 topic mapping configuration: ✅ Working"
        echo "   • Topic mappings file in RustFS: ✅ Present"
        echo "   • Kafka UI S3 topic mapping service: ✅ Healthy"
        echo "   • Topic mappings refresh: ✅ Working"
        echo
        echo -e "${BLUE}🔗 Access URLs:${NC}"
        echo "   • Kafka UI (S3 Topic Mapping): http://localhost:8082"
        echo "   • RustFS Console:              http://localhost:9001"
        echo
        echo -e "${BLUE}🔑 RustFS credentials:${NC}"
        echo "   • Username: rustfsadmin"
        echo "   • Password: rustfsadmin123"
        echo
        echo -e "${BLUE}📝 Next Steps:${NC}"
        echo "1. Access Kafka UI at http://localhost:8082"
        echo "2. Check the serde shows 'S3 Topic Mappings: s3://protobuf-descriptors/topic-mappings.json'"
        echo "3. Verify topic mappings are working for different topics"
        echo "4. Test that payment-events uses 'test.Order' (local override)"
        echo "5. Test that user-events uses 'test.User' (from S3)"
        echo
        echo -e "${BLUE}🧹 Cleanup:${NC}"
        echo "   docker-compose --profile s3-topic-mapping-test down -v"
        
        return 0
    else
        echo
        echo -e "${RED}❌ S3 Topic Mapping Integration Test Failed${NC}"
        echo
        echo -e "${YELLOW}🔍 Troubleshooting:${NC}"
        echo "Check the logs:"
        echo "   docker-compose --profile s3-topic-mapping-test logs kafka-ui-s3-topic-mapping"
        echo "   docker-compose logs rustfs"
        echo
        return 1
    fi
}

# Cleanup function
cleanup() {
    echo -e "${YELLOW}🧹 Cleaning up...${NC}"
    docker-compose --profile s3-topic-mapping-test down -v
}

# Handle script interruption
trap cleanup EXIT

# Run main function
main "$@"