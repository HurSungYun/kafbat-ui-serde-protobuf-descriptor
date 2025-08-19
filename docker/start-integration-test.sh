#!/bin/bash

set -e

echo "🚀 Starting Protobuf Descriptor Set Serde Integration Test"
echo "=========================================================="

# Check if we're in the right directory
if [[ ! -f "docker-compose.yml" ]]; then
    echo "❌ Error: docker-compose.yml not found. Run this script from the docker/ directory."
    exit 1
fi

# Check if JAR file exists
if [[ ! -f "../build/libs"/*.jar ]]; then
    echo "📦 Building serde JAR file..."
    cd .. && make build && cd docker
    echo "✅ JAR file built successfully"
else
    echo "✅ JAR file found"
fi

# Start the services
echo "🐳 Starting Docker Compose services..."
docker-compose up -d

echo "⏳ Waiting for services to be healthy..."
sleep 10

# Wait for services to be ready
echo "🔍 Checking service health..."
while ! docker-compose ps | grep -q "healthy"; do
    echo "   Waiting for services to start..."
    sleep 5
done

echo "✅ Services are running!"
echo ""
echo "🌐 kafbat UI: http://localhost:8080"
echo "📊 Kafka: localhost:9092"
echo ""
echo "📋 Next steps:"
echo "1. Open http://localhost:8080 in your browser"
echo "2. Check that 'ProtobufTestCluster' appears"
echo "3. Optionally start message producer:"
echo "   docker-compose --profile producer up -d producer"
echo ""
echo "🔍 To view logs:"
echo "   docker-compose logs -f [kafka-ui|kafka|producer]"
echo ""
echo "🛑 To stop:"
echo "   docker-compose down"

# Create test topics
echo ""
echo "📋 Creating test topics..."
docker-compose --profile setup run --rm topic-creator

# Send a test protobuf message
echo ""
echo "📤 Sending test protobuf message..."
./scripts/send_test_message.sh

echo "✅ Test message sent to user-events topic!"