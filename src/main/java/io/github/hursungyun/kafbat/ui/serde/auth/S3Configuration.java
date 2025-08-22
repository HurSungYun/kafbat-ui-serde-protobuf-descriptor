package io.github.hursungyun.kafbat.ui.serde.auth;

import io.kafbat.ui.serde.api.PropertyResolver;

import java.time.Duration;
import java.util.Optional;

/**
 * Configuration holder for S3 connection parameters
 * Centralizes S3 configuration parsing and validation
 */
public class S3Configuration {
    
    private final String endpoint;
    private final String bucket;
    private final String objectKey;
    private final Optional<String> accessKey;
    private final Optional<String> secretKey;
    private final String region;
    private final boolean secure;
    private final String stsEndpoint;
    private final Duration refreshInterval;

    private S3Configuration(String endpoint, String bucket, String objectKey,
                           Optional<String> accessKey, Optional<String> secretKey,
                           String region, boolean secure, String stsEndpoint,
                           Duration refreshInterval) {
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.objectKey = objectKey;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.secure = secure;
        this.stsEndpoint = stsEndpoint;
        this.refreshInterval = refreshInterval;
    }

    /**
     * Create S3 configuration from properties with standard prefixes
     */
    public static S3Configuration fromProperties(PropertyResolver properties) {
        return fromProperties(properties, "protobuf.s3");
    }

    /**
     * Create S3 configuration from properties with custom prefix
     */
    public static S3Configuration fromProperties(PropertyResolver properties, String prefix) {
        String endpoint = properties.getProperty(prefix + ".endpoint", String.class)
                .orElseThrow(() -> new IllegalArgumentException(prefix + ".endpoint is required"));
        String bucket = properties.getProperty(prefix + ".bucket", String.class)
                .orElseThrow(() -> new IllegalArgumentException(prefix + ".bucket is required"));
        String objectKey = properties.getProperty(prefix + ".object.key", String.class)
                .orElseThrow(() -> new IllegalArgumentException(prefix + ".object.key is required"));

        Optional<String> accessKey = properties.getProperty(prefix + ".access.key", String.class);
        Optional<String> secretKey = properties.getProperty(prefix + ".secret.key", String.class);
        String region = properties.getProperty(prefix + ".region", String.class).orElse(null);
        boolean secure = properties.getProperty(prefix + ".secure", Boolean.class).orElse(true);
        String stsEndpoint = properties.getProperty(prefix + ".sts.endpoint", String.class)
                .orElse("https://sts.amazonaws.com");
        Duration refreshInterval = properties.getProperty(prefix + ".refresh.interval.seconds", Long.class)
                .map(Duration::ofSeconds)
                .orElse(Duration.ofHours(1));

        return new S3Configuration(endpoint, bucket, objectKey, accessKey, secretKey,
                region, secure, stsEndpoint, refreshInterval);
    }

    // Getters
    public String getEndpoint() { return endpoint; }
    public String getBucket() { return bucket; }
    public String getObjectKey() { return objectKey; }
    public Optional<String> getAccessKey() { return accessKey; }
    public Optional<String> getSecretKey() { return secretKey; }
    public String getRegion() { return region; }
    public boolean isSecure() { return secure; }
    public String getStsEndpoint() { return stsEndpoint; }
    public Duration getRefreshInterval() { return refreshInterval; }
}