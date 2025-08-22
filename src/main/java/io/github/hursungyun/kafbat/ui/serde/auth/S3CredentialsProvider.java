package io.github.hursungyun.kafbat.ui.serde.auth;

import io.kafbat.ui.serde.api.PropertyResolver;
import io.minio.MinioClient;
import io.minio.credentials.Jwt;
import io.minio.credentials.Provider;
import io.minio.credentials.WebIdentityProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Handles S3 credential configuration for MinIO clients
 * Supports explicit credentials, environment variables, and IRSA (IAM Roles for Service Accounts)
 */
public class S3CredentialsProvider {

    /**
     * Configure MinIO client with appropriate credentials based on available configuration
     *
     * @param clientBuilder MinIO client builder to configure
     * @param properties    Property resolver for configuration
     */
    public static void configure(MinioClient.Builder clientBuilder, PropertyResolver properties) {
        Optional<String> accessKey = properties.getProperty("protobuf.s3.access.key", String.class);
        Optional<String> secretKey = properties.getProperty("protobuf.s3.secret.key", String.class);
        String stsEndpoint = properties.getProperty("protobuf.s3.sts.endpoint", String.class)
                .orElse("https://sts.amazonaws.com");

        configureCredentials(clientBuilder, accessKey, secretKey, stsEndpoint);
    }

    /**
     * Configure MinIO client credentials with explicit parameters
     *
     * @param clientBuilder MinIO client builder to configure
     * @param accessKey     Optional S3 access key
     * @param secretKey     Optional S3 secret key
     * @param stsEndpoint   STS endpoint for IRSA authentication
     */
    public static void configureCredentials(MinioClient.Builder clientBuilder,
                                          Optional<String> accessKey,
                                          Optional<String> secretKey,
                                          String stsEndpoint) {
        if (accessKey.isPresent() && secretKey.isPresent()) {
            // Priority 1: Use explicit credentials from configuration
            clientBuilder.credentials(accessKey.get(), secretKey.get());
        } else {
            // Priority 2: Try environment-based authentication
            configureEnvironmentBasedAuth(clientBuilder, stsEndpoint);
        }
    }

    /**
     * Configure authentication using environment variables or IRSA
     */
    private static void configureEnvironmentBasedAuth(MinioClient.Builder clientBuilder, String stsEndpoint) {
        String envAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String envSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String awsRoleArn = System.getenv("AWS_ROLE_ARN");
        String webIdentityTokenFile = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");

        if (envAccessKey != null && envSecretKey != null) {
            // Use environment credentials (from previous IRSA token refresh or manual setup)
            clientBuilder.credentials(envAccessKey, envSecretKey);
        } else if (awsRoleArn != null && webIdentityTokenFile != null) {
            // Use IRSA with WebIdentityProvider
            configureIrsaAuth(clientBuilder, awsRoleArn, webIdentityTokenFile, stsEndpoint);
        }
        // Otherwise rely on MinioClient default credential chain (instance profile, etc.)
    }

    /**
     * Configure IRSA (IAM Roles for Service Accounts) authentication
     */
    private static void configureIrsaAuth(MinioClient.Builder clientBuilder,
                                         String roleArn,
                                         String tokenFile,
                                         String stsEndpoint) {
        try {
            Supplier<Jwt> jwtSupplier = () -> {
                try {
                    String jwtToken = Files.readString(Path.of(tokenFile));
                    return new Jwt(jwtToken, 3600); // 1 hour expiry
                } catch (Exception e) {
                    throw new RuntimeException("Failed to read IRSA token from " + tokenFile, e);
                }
            };

            Provider provider = new WebIdentityProvider(
                    jwtSupplier,
                    stsEndpoint,                  // configurable STS endpoint
                    null,                         // duration (use default)
                    null,                         // policy (none)
                    roleArn,                      // role ARN from environment
                    "kafbat-ui-serde-session",    // session name
                    null                          // custom HTTP client (use default)
            );

            clientBuilder.credentialsProvider(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure IRSA WebIdentityProvider for role: " + roleArn, e);
        }
    }
}