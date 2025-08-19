#!/usr/bin/env python3
"""
Protobuf message producer for integration testing.
Produces sample User and Order messages to Kafka topics.
"""

import os
import time
import json
from kafka import KafkaProducer
from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.descriptor import FileDescriptor, Descriptor
from google.protobuf.message_factory import MessageFactory

def load_descriptors(descriptor_file_path):
    """Load protobuf descriptors from descriptor set file."""
    print(f"Loading descriptors from: {descriptor_file_path}")
    
    with open(descriptor_file_path, 'rb') as f:
        descriptor_set = FileDescriptorSet()
        descriptor_set.ParseFromString(f.read())
    
    # Build file descriptors
    descriptors = {}
    
    # First pass: files without dependencies
    for file_proto in descriptor_set.file:
        if not file_proto.dependency:
            file_desc = FileDescriptor(
                name=file_proto.name,
                package=file_proto.package,
                options=file_proto.options,
                serialized_pb=file_proto.SerializeToString()
            )
            descriptors[file_proto.name] = file_desc
    
    # Second pass: files with dependencies
    remaining = [fp for fp in descriptor_set.file if fp.dependency]
    while remaining:
        resolved_in_pass = []
        for file_proto in remaining:
            dependencies = [descriptors[dep] for dep in file_proto.dependency if dep in descriptors]
            if len(dependencies) == len(file_proto.dependency):
                file_desc = FileDescriptor(
                    name=file_proto.name,
                    package=file_proto.package,
                    options=file_proto.options,
                    serialized_pb=file_proto.SerializeToString(),
                    dependencies=dependencies
                )
                descriptors[file_proto.name] = file_desc
                resolved_in_pass.append(file_proto)
        
        for resolved in resolved_in_pass:
            remaining.remove(resolved)
            
        if not resolved_in_pass and remaining:
            print(f"Warning: Could not resolve dependencies for: {[fp.name for fp in remaining]}")
            break
    
    return descriptors

def create_sample_messages(descriptors):
    """Create sample protobuf messages."""
    factory = MessageFactory()
    
    # Get message classes
    user_desc = None
    order_desc = None
    address_desc = None
    
    for file_desc in descriptors.values():
        for message_desc in file_desc.message_types_by_name.values():
            if message_desc.name == 'User':
                user_desc = message_desc
            elif message_desc.name == 'Order':
                order_desc = message_desc
            elif message_desc.name == 'Address':
                address_desc = message_desc
    
    if not user_desc or not order_desc or not address_desc:
        raise ValueError("Required message types not found in descriptors")
    
    user_class = factory.GetPrototype(user_desc)
    order_class = factory.GetPrototype(order_desc)
    address_class = factory.GetPrototype(address_desc)
    
    messages = []
    
    # Create sample User messages
    for i in range(5):
        address = address_class()
        address.street = f"{100 + i * 10} Main St"
        address.city = "Springfield" if i % 2 == 0 else "Anytown"
        address.country = "USA"
        address.zip_code = 12345 + i
        
        user = user_class()
        user.id = 1000 + i
        user.name = f"User {i}"
        user.email = f"user{i}@example.com"
        user.tags.extend([f"tag{i}", "test"])
        user.type = 1 if i % 2 == 0 else 2  # ADMIN or REGULAR
        user.address.CopyFrom(address)
        
        messages.append(('user-events', user.SerializeToString(), f"User {i}"))
    
    # Create sample Order messages
    for i in range(3):
        # Create user for order
        address = address_class()
        address.street = f"{500 + i * 20} Oak Ave"
        address.city = "Commerce City"
        address.country = "USA" 
        address.zip_code = 54321 + i
        
        user = user_class()
        user.id = 2000 + i
        user.name = f"Customer {i}"
        user.email = f"customer{i}@example.com"
        user.type = 2  # REGULAR
        user.address.CopyFrom(address)
        
        # Create order
        order = order_class()
        order.id = 9000 + i
        order.user.CopyFrom(user)
        order.total_amount = 99.99 + (i * 25.50)
        order.status = 1  # CONFIRMED
        order.created_timestamp = int(time.time() * 1000) - (i * 3600000)  # hours ago
        
        # Add order items
        item = order.items.add()
        item.product_id = f"PROD-{1000 + i}"
        item.product_name = f"Product {i}"
        item.quantity = i + 1
        item.unit_price = 19.99 + (i * 5)
        
        messages.append(('order-events', order.SerializeToString(), f"Order {i}"))
    
    return messages

def main():
    """Main producer function."""
    print("Starting Protobuf message producer...")
    
    # Configuration
    kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')
    descriptor_file = os.getenv('DESCRIPTOR_FILE', '/descriptors/test_descriptors.desc')
    
    print(f"Kafka brokers: {kafka_brokers}")
    print(f"Descriptor file: {descriptor_file}")
    
    # Wait for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(10)
    
    try:
        # Load descriptors
        descriptors = load_descriptors(descriptor_file)
        print(f"Loaded {len(descriptors)} file descriptors")
        
        # Create sample messages
        messages = create_sample_messages(descriptors)
        print(f"Created {len(messages)} sample messages")
        
        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=[kafka_brokers],
            value_serializer=lambda x: x,  # Raw bytes
            key_serializer=str.encode
        )
        
        print("Producing messages to Kafka...")
        
        # Send messages
        for topic, message_bytes, description in messages:
            key = f"key-{int(time.time())}"
            future = producer.send(topic, key=key, value=message_bytes)
            result = future.get(timeout=10)
            print(f"Sent {description} to {topic} (partition={result.partition}, offset={result.offset})")
            time.sleep(1)
        
        producer.flush()
        producer.close()
        
        print("All messages sent successfully!")
        
        # Keep producing messages every 30 seconds
        print("Continuing to produce messages every 30 seconds...")
        while True:
            time.sleep(30)
            producer = KafkaProducer(
                bootstrap_servers=[kafka_brokers],
                value_serializer=lambda x: x,
                key_serializer=str.encode
            )
            
            # Send one message of each type
            user_msg = messages[0]  # First user message
            order_msg = messages[-1]  # Last order message
            
            for topic, message_bytes, description in [user_msg, order_msg]:
                key = f"key-{int(time.time())}"
                future = producer.send(topic, key=key, value=message_bytes)
                result = future.get(timeout=10)
                print(f"Sent {description} to {topic} (partition={result.partition}, offset={result.offset})")
            
            producer.flush()
            producer.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())