#!/bin/bash

set -e

echo "🚀 Starting Protobuf Descriptor Set Serde S3 Integration Test"
echo "============================================================="

# Check if we're in the right directory
if [[ ! -f "docker-compose.yml" ]]; then
    echo "❌ Error: docker-compose.yml not found. Run this script from the docker-compose/ directory."
    exit 1
fi

# Check if JAR file exists
if [[ ! -f "../build/libs"/*.jar ]]; then
    echo "📦 Building serde JAR file..."
    cd .. && ./gradlew build && cd docker-compose
    echo "✅ JAR file built successfully"
else
    echo "✅ JAR file found"
fi

# Start the core services first (kafka, zookeeper, minio)
echo "🐳 Starting core services (Kafka, Zookeeper, MinIO)..."
docker-compose up -d kafka zookeeper minio

echo "⏳ Waiting for core services to be healthy..."
sleep 15

# Check core service health
echo "🔍 Checking core service health..."
timeout=120
elapsed=0
while [ $elapsed -lt $timeout ]; do
    if docker-compose ps kafka | grep -q "healthy" && \
       docker-compose ps zookeeper | grep -q "healthy" && \
       docker-compose ps minio | grep -q "healthy"; then
        echo "✅ Core services are healthy!"
        break
    fi
    echo "   Waiting for services to start... ($elapsed/$timeout seconds)"
    sleep 5
    elapsed=$((elapsed + 5))
done

if [ $elapsed -ge $timeout ]; then
    echo "❌ Timeout waiting for services to become healthy"
    docker-compose logs
    exit 1
fi

# Setup MinIO with test data
echo "📁 Setting up MinIO with test descriptor file..."
docker-compose --profile setup run --rm minio-setup

# Verify descriptor was uploaded
echo "🔍 Verifying descriptor file in MinIO..."
docker-compose exec -T minio mc alias set minio http://localhost:9000 minioadmin minioadmin123 > /dev/null 2>&1
if docker-compose exec -T minio mc ls minio/protobuf-descriptors/ | grep -q test_descriptors.desc; then
    echo "✅ Descriptor file successfully uploaded to MinIO"
    echo "📋 MinIO bucket contents:"
    docker-compose exec -T minio mc ls minio/protobuf-descriptors/
else
    echo "❌ Failed to verify descriptor file in MinIO"
    echo "📋 Checking MinIO bucket contents:"
    docker-compose exec -T minio mc ls minio/protobuf-descriptors/ || echo "   Bucket listing failed"
    exit 1
fi

# Create test topics
echo "📋 Creating test topics..."
docker-compose --profile setup run --rm topic-creator

# Start Kafka UI with S3 configuration
echo "🌐 Starting Kafka UI with S3 descriptor source..."
docker-compose --profile s3-test up -d kafka-ui-s3

echo "⏳ Waiting for Kafka UI to be ready..."
sleep 20

# Wait for Kafka UI to be healthy
timeout=60
elapsed=0
while [ $elapsed -lt $timeout ]; do
    if docker-compose --profile s3-test ps kafka-ui-s3 | grep -q "healthy"; then
        echo "✅ Kafka UI with S3 is ready!"
        break
    fi
    echo "   Waiting for Kafka UI to start... ($elapsed/$timeout seconds)"
    sleep 5
    elapsed=$((elapsed + 5))
done

if [ $elapsed -ge $timeout ]; then
    echo "❌ Timeout waiting for Kafka UI to become healthy"
    echo "📋 Kafka UI logs:"
    docker-compose --profile s3-test logs kafka-ui-s3
    exit 1
fi

# Send test protobuf messages
echo "📤 Sending test protobuf messages..."
./scripts/send_test_message.sh

echo ""
echo "🎉 S3 Integration Test Setup Complete!"
echo "======================================"
echo ""
echo "🌐 Services available:"
echo "   • Kafka UI (S3):     http://localhost:8081"
echo "   • Kafka UI (Local):  http://localhost:8080 (if started separately)"
echo "   • MinIO Console:     http://localhost:9001"
echo "   • Kafka:             localhost:9092"
echo ""
echo "🔑 MinIO credentials:"
echo "   • Username: minioadmin"
echo "   • Password: minioadmin123"
echo ""
echo "📋 Test verification steps:"
echo "1. Open http://localhost:8081 in your browser"
echo "2. Navigate to 'ProtobufS3TestCluster'"
echo "3. Go to Topics → user-events"
echo "4. Verify that protobuf messages are properly deserialized to JSON"
echo "5. Check that the serde shows 'S3: s3://protobuf-descriptors/test_descriptors.desc' in description"
echo ""
echo "🔄 To test S3 refresh functionality:"
echo "1. Update the descriptor file in build/resources/test/"
echo "2. Upload it to MinIO: docker-compose exec minio mc cp /descriptors/test_descriptors.desc minio/protobuf-descriptors/"
echo "3. Wait 30 seconds (refresh interval) and verify changes are reflected"
echo ""
echo "🛑 To stop all services:"
echo "   docker-compose --profile s3-test down"
echo ""
echo "📊 To view logs:"
echo "   docker-compose --profile s3-test logs -f kafka-ui-s3"
echo "   docker-compose logs -f minio"