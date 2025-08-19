#!/usr/bin/env python3
"""
Simple script to create and send a protobuf message for testing.
"""

import sys
from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.descriptor import FileDescriptor
from google.protobuf.message_factory import MessageFactory

def create_test_user_message():
    """Create a simple User protobuf message."""
    
    # Load descriptors
    with open('/descriptors/test_descriptors.desc', 'rb') as f:
        descriptor_set = FileDescriptorSet()
        descriptor_set.ParseFromString(f.read())
    
    # Build file descriptors - simple version for user.proto
    user_file_desc = None
    for file_proto in descriptor_set.file:
        if file_proto.name == 'user.proto':
            user_file_desc = FileDescriptor(
                name=file_proto.name,
                package=file_proto.package,
                options=file_proto.options,
                serialized_pb=file_proto.SerializeToString()
            )
            break
    
    if not user_file_desc:
        raise ValueError("user.proto not found in descriptor set")
    
    # Get User message class
    factory = MessageFactory()
    
    # Find User and Address message descriptors
    user_desc = None
    address_desc = None
    for message_desc in user_file_desc.message_types_by_name.values():
        if message_desc.name == 'User':
            user_desc = message_desc
        elif message_desc.name == 'Address':
            address_desc = message_desc
    
    if not user_desc or not address_desc:
        raise ValueError("Required message types not found")
    
    user_class = factory.GetPrototype(user_desc)
    address_class = factory.GetPrototype(address_desc)
    
    # Create address
    address = address_class()
    address.street = "123 Test Street"
    address.city = "Test City"
    address.country = "USA"
    address.zip_code = 12345
    
    # Create user
    user = user_class()
    user.id = 999
    user.name = "Test User"
    user.email = "test@example.com"
    user.tags.extend(["test", "integration"])
    user.type = 1  # ADMIN
    user.address.CopyFrom(address)
    
    return user.SerializeToString()

if __name__ == '__main__':
    try:
        message_bytes = create_test_user_message()
        # Write binary data to stdout
        sys.stdout.buffer.write(message_bytes)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)