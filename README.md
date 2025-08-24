# Protobuf Descriptor Set Serde for Kafbat UI

A custom serializer/deserializer (serde) for [Kafbat UI](https://github.com/kafbat/kafka-ui) that allows deserializing protobuf messages using a protobuf descriptor set file.

## 📋 Requirements

- **Java 17+** (required by Kafbat UI Serde API)
- **Kafbat UI >= 0.7.0** (serde-api 1.0.0+)

## 🔗 Compatibility Matrix

| Plugin Version | Kafbat UI Version | Serde API Version | Java Version |
|---------------|-------------------|-------------------|--------------|
| 0.1.0+        | >= 0.7.0          | 1.0.0            | 17+          |

**Note**: This plugin uses `io.kafbat.ui:serde-api:1.0.0` from Maven Central.

## Demo

![Demo](demo.gif)

## Features

- **🔍 Protobuf Message Visualization**: Transform binary protobuf messages into readable JSON in Kafbat UI
- **📝 Message Production**: Create protobuf messages from JSON in Kafka UI (full serialization support)
- **📋 Descriptor Set Support**: Use compiled protobuf descriptor sets (`.desc` files) for schema definitions
- **🎯 Topic-Specific Mapping**: Configure different protobuf message types for different Kafka topics
- **📁 Flexible Storage**: Load descriptors from local files or S3-compatible storage (AWS S3, MinIO)

## Quick Start

### 1. Generate Descriptor Set

Create a protobuf descriptor set from your .proto files:

```bash
# Using buf (recommended)
buf build -o descriptors.desc

# Using protoc
protoc --descriptor_set_out=descriptors.desc \
       --include_imports \
       your_proto_files.proto
```

**🚨 CRITICAL: The `--include_imports` flag is MANDATORY if your .proto files have any imports or dependencies**

### 2. Download the Serde

Download the latest JAR from [Releases](https://github.com/hursungyun/kafka-ui-protobuf-descriptor-set-serde/releases) or build from source.

**Note**: Replace `{VERSION}` in the configuration examples below with the actual version number (e.g., `0.1.0`).

### 3. Configure Kafbat UI

Add the serde to your Kafbat UI configuration:

#### Local File Configuration

```yaml
kafka:
  clusters:
    - name: MyCluster
      serde:
        - name: ProtobufDescriptorSetSerde
          className: io.github.hursungyun.kafbat.ui.serde.ProtobufDescriptorSetSerde
          filePath: /path/to/kafbat-ui-serde-protobuf-descriptor-{VERSION}.jar
          properties:
            descriptor.file: /path/to/your/descriptors.desc
            message.default.type: "your.package.DefaultMessage"
            topic.mapping.local:
              user-events: "your.package.User"
              order-events: "your.package.Order"
```

#### S3 Configuration

```yaml
kafka:
  clusters:
    - name: MyCluster
      serde:
        - name: ProtobufDescriptorSetSerde
          className: io.github.hursungyun.kafbat.ui.serde.ProtobufDescriptorSetSerde
          filePath: /path/to/kafbat-ui-serde-protobuf-descriptor-{VERSION}.jar
          properties:
            # S3 Configuration
            descriptor.s3.endpoint: "https://s3.amazonaws.com"
            descriptor.s3.bucket: "my-protobuf-descriptors"
            descriptor.s3.object.key: "descriptors/my-app.desc"
            descriptor.s3.access.key: "YOUR_ACCESS_KEY"
            descriptor.s3.secret.key: "YOUR_SECRET_KEY"
            descriptor.s3.region: "us-east-1"
            descriptor.s3.refresh.interval.seconds: 300
            
            # Message Configuration
            message.default.type: "your.package.DefaultMessage"
            
            # S3 Topic Mapping (optional)
            topic.mapping.s3.bucket: "my-protobuf-descriptors"
            topic.mapping.s3.object.key: "topic-mappings.json"
            
            # Local Topic Mapping (overrides S3 config)
            topic.mapping.local:
              user-events: "your.package.User"
              order-events: "your.package.Order"
```

#### S3 Topic Mapping JSON Format

When using S3 topic mappings, create a JSON file with topic-to-message-type mappings:

```json
{
  "user-events": "your.package.User",
  "order-events": "your.package.Order",
  "payment-events": "your.package.Payment"
}
```

**Note**: Local `topic.mapping.local` configuration always overrides S3 topic mappings.

## ⚡ Features & Roadmap

### Current Version (0.1.0)
- **✅ Deserialization**: Full protobuf message deserialization (binary → JSON)
- **✅ Serialization**: Full protobuf message serialization (JSON → binary)
- **✅ Strict Validation**: Unknown JSON fields cause serialization errors
- **✅ Required Field Validation**: Automatic validation for proto2 required fields
- **❌ Key/Value Separation**: Currently only supports message values, not keys

### Planned Features (v0.2.0+)
- **🔑 Key Support**: Separate protobuf types for message keys and values
- **📊 Metrics**: Performance monitoring and observability
- **🔄 Schema Evolution**: Compatibility checking and migration support

#### S3 IAM Role-based Authentication (IRSA)

For AWS environments using IAM roles (like EKS with IRSA), you can omit the access keys:

```yaml
kafka:
  clusters:
    - name: MyCluster
      serde:
        - name: ProtobufDescriptorSetSerde
          className: io.github.hursungyun.kafbat.ui.serde.ProtobufDescriptorSetSerde
          filePath: /path/to/kafbat-ui-serde-protobuf-descriptor-{VERSION}.jar
          properties:
            # S3 Configuration using IAM roles (no access keys needed)
            descriptor.s3.endpoint: "https://s3.ap-northeast-2.amazonaws.com"
            descriptor.s3.bucket: "my-protobuf-descriptors"
            descriptor.s3.object.key: "descriptors/my-app.desc"
            descriptor.s3.region: "ap-northeast-2"
            descriptor.s3.sts.endpoint: "https://sts.amazonaws.com"  # Optional: STS endpoint for IRSA
            descriptor.s3.refresh.interval.seconds: 6000
            
            # Topic mappings from local configuration
            topic.mapping.local:
              "my-topic": "my.package.MyMessage"
```

## Configuration Properties

### Required Properties

**Either local file OR S3 configuration must be provided. S3 takes precedence if both are specified.**

| Property | Description |
|----------|-------------|
| `descriptor.file` | Path to local protobuf descriptor set file |
| `descriptor.s3.endpoint` | S3 endpoint URL (e.g., https://s3.amazonaws.com) |
| `descriptor.s3.bucket` | S3 bucket name containing the descriptor set |
| `descriptor.s3.object.key` | S3 object key (path) to the descriptor set file |
| `descriptor.s3.access.key` | S3 access key (optional - uses IAM roles if not provided) |
| `descriptor.s3.secret.key` | S3 secret key (optional - uses IAM roles if not provided) |
| `descriptor.s3.sts.endpoint` | STS endpoint for IRSA (default: https://sts.amazonaws.com) |

### Optional Properties

| Property | Default | Description |
|----------|---------|-------------|
| `message.default.type` | - | Default message type for all topics |
| `topic.mapping.local.*` | - | Topic-specific message type mapping (overrides S3 config) |
| `topic.mapping.s3.bucket` | - | S3 bucket containing topic mapping JSON file |
| `topic.mapping.s3.object.key` | - | S3 object key for topic mapping JSON file |
| `descriptor.s3.region` | - | S3 region (if required by your provider) |
| `descriptor.s3.secure` | `true` | Use HTTPS (set to false for HTTP endpoints) |
| `descriptor.s3.sts.endpoint` | `https://sts.amazonaws.com` | STS endpoint for IRSA authentication |
| `descriptor.s3.refresh.interval.seconds` | `300` | How often to check for descriptor updates |

## Support

- **📚 Documentation**: See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup
- **🐛 Issues**: Report problems on [GitHub Issues](https://github.com/hursungyun/kafka-ui-protobuf-descriptor-set-serde/issues)
- **💡 Features**: Request new features via GitHub Issues
- **❓ Questions**: Check existing issues or create a new one

## License

This project is licensed under the MIT License - see the LICENSE file for details.