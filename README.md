# Protobuf Descriptor Set Serde for Kafbat UI

A custom serializer/deserializer (serde) for Kafbat UI that allows deserializing protobuf messages using a protobuf descriptor set file.

## Features

- Load protobuf message definitions from a descriptor set file
- Deserialize protobuf binary data to JSON format

## Generating Descriptor Set Files

To generate a descriptor set file from your `.proto` files:

```bash
protoc --descriptor_set_out=descriptors.desc \
       --include_imports \
       your_proto_files.proto
```

The `--include_imports` flag ensures all dependencies are included in the descriptor set.

## Configuration

Add this serde to your Kafbat UI configuration:

```yaml
kafka:
  clusters:
    - name: MyCluster
      serde:
        - name: ProtobufDescriptorSetSerde
          className: io.github.hursungyun.kafbat.ui.serde.ProtobufDescriptorSetSerde
          filePath: /var/lib/path/to/your-serde.jar
          properties:
            protobuf.descriptor.set.file: /path/to/your/descriptors.desc
            # Default message type for all topics
            protobuf.message.name: "your.package.DefaultMessage"
            # Topic-specific mappings (simple key: value format)
            protobuf.topic.message.map:
              user-events: "your.package.User"
              order-events: "your.package.Order"
              product-updates: "your.package.Product"
```

## Properties

| Property | Required | Description |
|----------|----------|-------------|
| `protobuf.descriptor.set.file` | Yes | Path to the protobuf descriptor set file |
| `protobuf.message.name` | No | Default message type for all topics |
| `protobuf.topic.message.map.*` | No | Topic-specific message type mapping (key: value format) |

## Building

Build the serde using Gradle:

```bash
./gradlew build
```

This will create a shadow jar in `build/libs/` that can be used with Kafbat UI.


## How It Works

1. The serde loads the descriptor set file during configuration
2. When deserializing, it attempts to parse the binary data with each message type in the descriptor set
3. When successful, it converts the protobuf message to JSON format
4. Returns metadata including the message type name and source file
5. If no message type matches, it returns the raw bytes as a hex string

## Limitations

- Serialization is not currently implemented (deserialization only)
- The serde tries all message types sequentially, which may impact performance with large descriptor sets
- No schema inference - relies on trying different message types