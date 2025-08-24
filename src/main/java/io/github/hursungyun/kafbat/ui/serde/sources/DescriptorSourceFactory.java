package io.github.hursungyun.kafbat.ui.serde.sources;

import io.kafbat.ui.serde.api.PropertyResolver;
import io.github.hursungyun.kafbat.ui.serde.auth.MinioClientFactory;
import io.github.hursungyun.kafbat.ui.serde.auth.S3Configuration;
import io.minio.MinioClient;

import java.util.Optional;

/**
 * Factory for creating descriptor sources based on configuration
 */
public class DescriptorSourceFactory {

    public static DescriptorSource create(PropertyResolver properties) {
        // Check for S3 configuration first
        Optional<String> s3Endpoint = properties.getProperty("descriptor.value.s3.endpoint", String.class);
        Optional<String> s3Bucket = properties.getProperty("descriptor.value.s3.bucket", String.class);
        Optional<String> s3ObjectKey = properties.getProperty("descriptor.value.s3.object.key", String.class);

        if (s3Endpoint.isPresent() && s3Bucket.isPresent() && s3ObjectKey.isPresent()) {
            // S3 configuration found
            return createS3Source(properties, s3Endpoint.get(), s3Bucket.get(), s3ObjectKey.get());
        }

        // Fall back to local file
        Optional<String> filePath = properties.getProperty("descriptor.value.file", String.class);
        if (filePath.isPresent()) {
            return new LocalFileDescriptorSource(filePath.get());
        }

        throw new IllegalArgumentException(
            "Either descriptor.value.file or S3 configuration " +
            "(descriptor.value.s3.endpoint, descriptor.value.s3.bucket, descriptor.value.s3.object.key) must be provided");
    }

    private static DescriptorSource createS3Source(PropertyResolver properties, String endpoint,
                                                  String bucket, String objectKey) {
        // Create S3 configuration from properties
        S3Configuration config = S3Configuration.fromProperties(properties, "descriptor.value.s3");

        // Create MinIO client using the factory
        MinioClient minioClient = MinioClientFactory.create(config);

        return new S3DescriptorSource(minioClient, bucket, objectKey, config.getRefreshInterval());
    }
}