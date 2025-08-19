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

# Optional: Start producer if requested
if [[ "$1" == "--with-producer" ]]; then
    echo ""
    echo "🔄 Starting message producer..."
    docker-compose --profile producer up -d producer
    echo "✅ Producer started! Messages will be generated every 30 seconds."
    echo "📝 View producer logs: docker-compose logs -f producer"
fi

# Send a test protobuf message
echo ""
echo "📤 Sending test protobuf message..."
docker run --rm --network docker_default -v "$(pwd)/descriptors:/descriptors" -v "$(pwd)/scripts:/scripts" python:3.11-slim bash -c "
    pip install protobuf==4.24.4 > /dev/null 2>&1 && 
    cd /scripts && 
    python test_protobuf_message.py 2>/dev/null
" | docker exec -i kafka-protobuf-test kafka-console-producer --bootstrap-server kafka:29092 --topic user-events 2>/dev/null

echo "✅ Test message sent to user-events topic!"