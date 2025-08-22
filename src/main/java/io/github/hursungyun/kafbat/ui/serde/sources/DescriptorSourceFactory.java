package io.github.hursungyun.kafbat.ui.serde.sources;

import io.kafbat.ui.serde.api.PropertyResolver;
import io.minio.MinioClient;
import io.minio.credentials.Jwt;
import io.minio.credentials.Provider;
import io.minio.credentials.WebIdentityProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

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

        // Get S3 credentials (optional for IAM role-based authentication)
        Optional<String> accessKey = properties.getProperty("protobuf.s3.access.key", String.class);
        Optional<String> secretKey = properties.getProperty("protobuf.s3.secret.key", String.class);

        // Optional configuration
        String region = properties.getProperty("protobuf.s3.region", String.class).orElse(null);
        boolean secure = properties.getProperty("protobuf.s3.secure", Boolean.class).orElse(true);
        String stsEndpoint = properties.getProperty("protobuf.s3.sts.endpoint", String.class)
                .orElse("https://sts.amazonaws.com");
        Duration refreshInterval = properties.getProperty("protobuf.s3.refresh.interval.seconds", Long.class)
                .map(Duration::ofSeconds)
                .orElse(Duration.ofHours(1)); // Default 1 hour

        // Build MinIO client
        MinioClient.Builder clientBuilder = MinioClient.builder()
                .endpoint(endpoint);

        // Configure credentials using centralized logic
        configureMinioCredentials(clientBuilder, accessKey, secretKey, stsEndpoint);

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

    /**
     * Centralized method to configure MinIO client credentials
     * Handles explicit credentials, IRSA/IAM roles, and default credential chain
     */
    public static void configureMinioCredentials(MinioClient.Builder clientBuilder,
                                               Optional<String> accessKey,
                                               Optional<String> secretKey,
                                               String stsEndpoint) {
        if (accessKey.isPresent() && secretKey.isPresent()) {
            // Use explicit credentials from configuration
            clientBuilder.credentials(accessKey.get(), secretKey.get());
        } else {
            // Try to get credentials from environment (IRSA/IAM roles)
            String envAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
            String envSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
            String envSessionToken = System.getenv("AWS_SESSION_TOKEN");
            String awsRoleArn = System.getenv("AWS_ROLE_ARN");
            String webIdentityTokenFile = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");

            if (envAccessKey != null && envSecretKey != null) {
                // Use environment credentials (from previous IRSA token refresh)
                clientBuilder.credentials(envAccessKey, envSecretKey);
            } else if (awsRoleArn != null && webIdentityTokenFile != null) {
                // Use IRSA with WebIdentityProvider
                try {
                    Supplier<Jwt> jwtSupplier = () -> {
                        try {
                            String jwtToken = Files.readString(Path.of(webIdentityTokenFile));
                            return new Jwt(jwtToken, 3600); // 1 hour expiry
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to read IRSA token from " + webIdentityTokenFile, e);
                        }
                    };

                    Provider provider = new WebIdentityProvider(
                            jwtSupplier,
                            stsEndpoint,                  // configurable STS endpoint
                            null,                         // duration (use default)
                            null,                         // policy (none)
                            awsRoleArn,                   // role ARN from environment
                            "kafbat-ui-serde-session",    // session name
                            null                          // custom HTTP client (use default)
                    );

                    clientBuilder.credentialsProvider(provider);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to configure IRSA WebIdentityProvider for role: " + awsRoleArn, e);
                }
            }
            // Otherwise rely on MinioClient default credential chain (instance profile, etc.)
        }
    }
}