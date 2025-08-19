#!/bin/bash

# Simple script to send a valid protobuf message using printf
# Creates a User message with basic fields

echo "Sending simple protobuf User message..."

# Create a simple User protobuf message manually
# Field 1 (id): varint 123 = 0x08 0x7B
# Field 2 (name): string "Test" = 0x12 0x04 "Test"  
# This is a minimal valid protobuf message

printf '\x08\x7b\x12\x04Test' | docker exec -i kafka-protobuf-test kafka-console-producer --bootstrap-server kafka:29092 --topic user-events

echo "Simple protobuf message sent to user-events topic!"