# Protobuf Descriptor Set Serde for Kafbat UI

A custom serializer/deserializer (serde) for [Kafbat UI](https://github.com/kafbat/kafka-ui) that allows deserializing protobuf messages using a protobuf descriptor set file.

## Demo

![Demo](demo.gif)

## Features

- Load protobuf message definitions from a descriptor set file (local or S3)
- Deserialize protobuf binary data to JSON format
- Topic-to-message-type mapping for precise deserialization
- S3 support with automatic caching and refresh capabilities
- MinIO compatibility for self-hosted S3-compatible storage

## Generating Descriptor Set Files

Using buf:
```bash
buf build -o descriptors.desc
```

Using protoc:
```bash
protoc --descriptor_set_out=descriptors.desc \
       --include_imports \
       your_proto_files.proto
```

**🚨 CRITICAL: The `--include_imports` flag is MANDATORY if your .proto files have any imports or dependencies**

## Configuration

Add this serde to your Kafbat UI configuration:

### Local File Configuration

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

### S3 Configuration

```yaml
kafka:
  clusters:
    - name: MyCluster
      serde:
        - name: ProtobufDescriptorSetSerde
          className: io.github.hursungyun.kafbat.ui.serde.ProtobufDescriptorSetSerde
          filePath: /var/lib/path/to/your-serde.jar
          properties:
            # S3 Configuration (takes precedence over local file)
            protobuf.s3.endpoint: "https://s3.amazonaws.com"
            protobuf.s3.bucket: "my-protobuf-descriptors"
            protobuf.s3.object.key: "descriptors/my-app.desc"
            protobuf.s3.access.key: "YOUR_ACCESS_KEY"
            protobuf.s3.secret.key: "YOUR_SECRET_KEY"
            protobuf.s3.region: "us-east-1"  # optional
            protobuf.s3.secure: true  # optional, default: true
            protobuf.s3.refresh.interval.seconds: 300  # optional, default: 300 (5 minutes)
            
            # Message type configuration (same as local file)
            protobuf.message.name: "your.package.DefaultMessage"
            protobuf.topic.message.map:
              user-events: "your.package.User"
              order-events: "your.package.Order"
              product-updates: "your.package.Product"
```

### MinIO Configuration

For self-hosted MinIO or other S3-compatible services:

```yaml
properties:
  protobuf.s3.endpoint: "http://minio.example.com:9000"
  protobuf.s3.secure: false  # Use HTTP instead of HTTPS
  protobuf.s3.bucket: "protobuf-descriptors"
  protobuf.s3.object.key: "app-descriptors.desc"
  protobuf.s3.access.key: "minio-access-key"
  protobuf.s3.secret.key: "minio-secret-key"
  protobuf.s3.refresh.interval.seconds: 60  # Check for updates every minute
```

## Properties

### Local File Properties

| Property | Required | Description |
|----------|----------|-------------|
| `protobuf.descriptor.set.file` | Yes* | Path to the protobuf descriptor set file |

### S3 Properties

| Property | Required | Description |
|----------|----------|-------------|
| `protobuf.s3.endpoint` | Yes* | S3 endpoint URL (e.g., https://s3.amazonaws.com or http://minio:9000) |
| `protobuf.s3.bucket` | Yes* | S3 bucket name containing the descriptor set |
| `protobuf.s3.object.key` | Yes* | S3 object key (path) to the descriptor set file |
| `protobuf.s3.access.key` | Yes* | S3 access key |
| `protobuf.s3.secret.key` | Yes* | S3 secret key |
| `protobuf.s3.region` | No | S3 region (if required by your S3 provider) |
| `protobuf.s3.secure` | No | Use HTTPS (default: true). Set to false for HTTP endpoints |
| `protobuf.s3.refresh.interval.seconds` | No | How often to check for descriptor updates (default: 300 seconds) |

### Message Configuration Properties

| Property | Required | Description |
|----------|----------|-------------|
| `protobuf.message.name` | No | Default message type for all topics |
| `protobuf.topic.message.map.*` | No | Topic-specific message type mapping (key: value format) |

*Either local file OR S3 configuration must be provided. S3 configuration takes precedence if both are specified.

## Building

Build the serde using Gradle:

```bash
./gradlew build
```

This will create a shadow jar in `build/libs/` that can be used with Kafbat UI.


## How It Works

1. The serde loads the descriptor set from the configured source (local file or S3) during configuration
2. For S3 sources, the descriptor set is cached with automatic refresh based on the configured interval
3. When deserializing, it uses the configured message type for the topic to parse the binary data
4. When successful, it converts the protobuf message to JSON format
5. Returns metadata including the message type name and source file
6. For S3 sources, the serde periodically checks for updates and refreshes the cache automatically

## S3 Upload Examples

### Using AWS CLI

```bash
# Upload descriptor set to S3
aws s3 cp descriptors.desc s3://my-protobuf-descriptors/descriptors/my-app.desc
```

### Using MinIO Client (mc)

```bash
# Configure MinIO client
mc alias set myminio http://minio.example.com:9000 ACCESS_KEY SECRET_KEY

# Upload descriptor set
mc cp descriptors.desc myminio/protobuf-descriptors/app-descriptors.desc
```

## Limitations

- Serialization is not currently implemented (deserialization only)
- Requires explicit topic-to-message-type mapping configuration
- Only supports VALUE target (KEY deserialization not implemented)
- S3 credentials must be provided in configuration (no IAM roles or temporary credentials support yet)
