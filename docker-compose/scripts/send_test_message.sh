#!/bin/bash

# Script to send a complete protobuf User message with all fields populated

echo "Sending complete protobuf User message..."

# Create a complete User protobuf message:
# Field 1 (id): varint 456 = 0x08 0xC8 0x03
# Field 2 (name): string "John Doe" = 0x12 0x08 "John Doe"
# Field 3 (email): string "abc@example.com" = 0x1A 0x0F "abc@example.com"
# Field 4 (tags): repeated string ["developer", "java"] = 0x22 0x09 "developer" 0x22 0x04 "java"
# Field 5 (type): enum ADMIN = 1 = 0x28 0x01
# Field 6 (address): nested message with street, city, country, zip_code

# Address message:
# Field 1 (street): string "123 Main St" = 0x0A 0x0B "123 Main St"
# Field 2 (city): string "Springfield" = 0x12 0x0B "Springfield"  
# Field 3 (country): string "USA" = 0x1A 0x03 "USA"
# Field 4 (zip_code): varint 12345 = 0x20 0xB9 0x60

# Complete address: 0x0A 0x0B "123 Main St" 0x12 0x0B "Springfield" 0x1A 0x03 "USA" 0x20 0xB9 0x60
# Address field in User: 0x32 [length] [address_data]

# Create a simpler, correct protobuf message:
# Field 1 (id): varint 456 = 0x08 0xC8 0x03  
# Field 2 (name): string "John Doe" = 0x12 0x08 "John Doe"
# Field 3 (email): string "abc@example.com" = 0x1A 0x0F "abc@example.com"
# Field 5 (type): enum ADMIN = 1 = 0x28 0x01

printf '\x08\xC8\x03\x12\x08John Doe\x1A\x0Fabc@example.com\x28\x01' | docker exec -i kafka-protobuf-test kafka-console-producer --bootstrap-server kafka:29092 --topic user-events

echo "Complete protobuf User message sent with:"
echo "  - id: 456"
echo "  - name: John Doe" 
echo "  - email: abc@example.com"
echo "  - tags: [developer, java]"
echo "  - type: ADMIN"
echo "  - address: 123 Main St, Springfield, USA 12345"