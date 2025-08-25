package io.github.hursungyun.kafbat.ui.serde.auth;

import io.minio.MinioClient;

/**
 * Factory for creating configured MinIO clients Separates client creation logic from business logic
 */
public class MinioClientFactory {

    /**
     * Create a MinIO client from S3 configuration
     *
     * @param config S3 configuration containing endpoint, credentials, and other settings
     * @return Configured MinioClient ready for use
     */
    public static MinioClient create(S3Configuration config) {
        MinioClient.Builder clientBuilder = MinioClient.builder().endpoint(config.getEndpoint());

        // Configure authentication
        S3CredentialsProvider.configureCredentials(
                clientBuilder,
                config.getAccessKey(),
                config.getSecretKey(),
                config.getStsEndpoint());

        // Configure region
        if (config.getRegion() != null) {
            clientBuilder.region(config.getRegion());
        }

        // Configure SSL
        if (!config.isSecure()) {
            String endpoint = config.getEndpoint();
            if (endpoint.startsWith("https://")) {
                endpoint = endpoint.replace("https://", "http://");
                clientBuilder.endpoint(endpoint);
            }
        }

        return clientBuilder.build();
    }
}
