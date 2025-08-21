package io.github.hursungyun.kafbat.ui.serde;

import io.kafbat.ui.serde.api.PropertyResolver;
import io.minio.MinioClient;

import java.time.Duration;
import java.util.Optional;

/**
 * Factory for creating descriptor sources based on configuration
 */
public class DescriptorSourceFactory {
    
    public static DescriptorSource create(PropertyResolver properties) {
        // Check for S3 configuration first
        Optional<String> s3Endpoint = properties.getProperty("protobuf.s3.endpoint", String.class);
        Optional<String> s3Bucket = properties.getProperty("protobuf.s3.bucket", String.class);
        Optional<String> s3ObjectKey = properties.getProperty("protobuf.s3.object.key", String.class);
        
        if (s3Endpoint.isPresent() && s3Bucket.isPresent() && s3ObjectKey.isPresent()) {
            // S3 configuration found
            return createS3Source(properties, s3Endpoint.get(), s3Bucket.get(), s3ObjectKey.get());
        }
        
        // Fall back to local file
        Optional<String> filePath = properties.getProperty("protobuf.descriptor.set.file", String.class);
        if (filePath.isPresent()) {
            return new LocalFileDescriptorSource(filePath.get());
        }
        
        throw new IllegalArgumentException(
            "Either protobuf.descriptor.set.file or S3 configuration " +
            "(protobuf.s3.endpoint, protobuf.s3.bucket, protobuf.s3.object.key) must be provided");
    }
    
    private static DescriptorSource createS3Source(PropertyResolver properties, String endpoint, 
                                                  String bucket, String objectKey) {
        
        // Get S3 credentials
        String accessKey = properties.getProperty("protobuf.s3.access.key", String.class)
                .orElseThrow(() -> new IllegalArgumentException("protobuf.s3.access.key is required for S3 source"));
        String secretKey = properties.getProperty("protobuf.s3.secret.key", String.class)
                .orElseThrow(() -> new IllegalArgumentException("protobuf.s3.secret.key is required for S3 source"));
        
        // Optional configuration
        String region = properties.getProperty("protobuf.s3.region", String.class).orElse(null);
        boolean secure = properties.getProperty("protobuf.s3.secure", Boolean.class).orElse(true);
        Duration refreshInterval = properties.getProperty("protobuf.s3.refresh.interval.seconds", Long.class)
                .map(Duration::ofSeconds)
                .orElse(Duration.ofMinutes(5)); // Default 5 minutes
        
        // Build MinIO client
        MinioClient.Builder clientBuilder = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey);
        
        if (region != null) {
            clientBuilder.region(region);
        }
        
        // Configure SSL
        if (!secure) {
            // For non-HTTPS endpoints, we need to ensure the endpoint doesn't start with https://
            if (endpoint.startsWith("https://")) {
                endpoint = endpoint.replace("https://", "http://");
                clientBuilder.endpoint(endpoint);
            }
        }
        
        MinioClient minioClient = clientBuilder.build();
        
        return new S3DescriptorSource(minioClient, bucket, objectKey, refreshInterval);
    }
}