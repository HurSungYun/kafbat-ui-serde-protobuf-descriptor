# S3 Configuration Examples

This document provides practical examples of configuring the Protobuf Descriptor Set Serde with S3 sources.

## AWS S3 Configuration

```yaml
kafka:
  clusters:
    - name: MyCluster
      serde:
        - name: ProtobufDescriptorSetSerde
          className: io.github.hursungyun.kafbat.ui.serde.ProtobufDescriptorSetSerde
          filePath: /var/lib/kafka-ui/protobuf-descriptor-serde.jar
          properties:
            # AWS S3 Configuration
            protobuf.s3.endpoint: "https://s3.amazonaws.com"
            protobuf.s3.bucket: "my-company-protobuf-schemas"
            protobuf.s3.object.key: "prod/kafka-schemas/v1.2.3/descriptors.desc"
            protobuf.s3.access.key: "${AWS_ACCESS_KEY_ID}"
            protobuf.s3.secret.key: "${AWS_SECRET_ACCESS_KEY}"
            protobuf.s3.region: "us-east-1"
            protobuf.s3.refresh.interval.seconds: 300  # 5 minutes
            
            # Message Configuration
            protobuf.message.name: "com.company.events.DefaultEvent"
            protobuf.topic.message.map:
              user-created: "com.company.user.UserCreatedEvent"
              order-placed: "com.company.order.OrderPlacedEvent"
              payment-processed: "com.company.payment.PaymentProcessedEvent"
```

## MinIO Self-hosted Configuration

```yaml
kafka:
  clusters:
    - name: DevelopmentCluster
      serde:
        - name: ProtobufDescriptorSetSerde
          className: io.github.hursungyun.kafbat.ui.serde.ProtobufDescriptorSetSerde
          filePath: /var/lib/kafka-ui/protobuf-descriptor-serde.jar
          properties:
            # MinIO Configuration
            protobuf.s3.endpoint: "http://minio.internal.company.com:9000"
            protobuf.s3.bucket: "dev-protobuf-schemas"
            protobuf.s3.object.key: "schemas/latest/descriptors.desc"
            protobuf.s3.access.key: "dev-access-key"
            protobuf.s3.secret.key: "dev-secret-key"
            protobuf.s3.secure: false  # HTTP endpoint
            protobuf.s3.refresh.interval.seconds: 60  # Check every minute in dev
            
            # Message Configuration  
            protobuf.message.name: "com.company.events.TestEvent"
```

## Google Cloud Storage (via S3 API)

```yaml
properties:
  # GCS with S3-compatible API
  protobuf.s3.endpoint: "https://storage.googleapis.com"
  protobuf.s3.bucket: "my-gcs-bucket"
  protobuf.s3.object.key: "protobuf/schemas/descriptors.desc"
  protobuf.s3.access.key: "${GCS_ACCESS_KEY}"
  protobuf.s3.secret.key: "${GCS_SECRET_KEY}"
  protobuf.s3.refresh.interval.seconds: 600  # 10 minutes
```

## Uploading Descriptors to S3

### Using AWS CLI
```bash
# Upload to AWS S3
aws s3 cp descriptors.desc s3://my-company-protobuf-schemas/prod/kafka-schemas/v1.2.3/descriptors.desc

# Set proper permissions
aws s3api put-object-acl --bucket my-company-protobuf-schemas --key prod/kafka-schemas/v1.2.3/descriptors.desc --acl private
```

### Using MinIO Client
```bash
# Configure MinIO client
mc alias set company-minio http://minio.internal.company.com:9000 dev-access-key dev-secret-key

# Upload descriptor set
mc cp descriptors.desc company-minio/dev-protobuf-schemas/schemas/latest/descriptors.desc

# Verify upload
mc ls company-minio/dev-protobuf-schemas/schemas/latest/
```

## Benefits of S3 Storage

1. **Centralized Management**: Store protobuf schemas in a central location accessible by multiple Kafka UI instances
2. **Version Control**: Use S3 object versioning to maintain schema history
3. **Automatic Refresh**: Schemas are automatically refreshed without restarting Kafka UI
4. **High Availability**: S3's durability and availability guarantees
5. **Access Control**: Fine-grained IAM permissions for schema access
6. **Backup & Recovery**: Built-in backup and cross-region replication

## Refresh Behavior

The serde automatically refreshes descriptors from S3 based on:

1. **Time-based**: Controlled by `protobuf.s3.refresh.interval.seconds`
2. **ETag checking**: Only downloads if the S3 object has changed
3. **Caching**: Keeps descriptors in memory between refreshes for performance

## Troubleshooting

### Common Issues

1. **Connection Failed**: Check endpoint URL and network connectivity
2. **Access Denied**: Verify S3 credentials and bucket permissions
3. **Object Not Found**: Confirm bucket name and object key are correct
4. **SSL Errors**: For self-hosted solutions, set `protobuf.s3.secure: false` for HTTP endpoints

### Debug Logging

Enable debug logging to troubleshoot S3 connectivity:

```yaml
logging:
  level:
    io.github.hursungyun.kafbat.ui.serde: DEBUG
    io.minio: DEBUG
```