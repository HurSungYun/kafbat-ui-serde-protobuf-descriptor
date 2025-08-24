package io.github.hursungyun.kafbat.ui.serde.auth;

import io.kafbat.ui.serde.api.PropertyResolver;
import io.minio.MinioClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class MinioClientFactoryTest {

    @Mock
    private PropertyResolver properties;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void shouldCreateMinioClientFromConfiguration() {
        // Setup properties for valid S3 configuration
        setupValidS3Properties();

        // Create configuration and client
        S3Configuration config = S3Configuration.fromProperties(properties);
        MinioClient client = MinioClientFactory.create(config);

        // Verify client was created successfully
        assertThat(client).isNotNull();
    }

    @Test
    void shouldCreateMinioClientWithMinimalConfiguration() {
        // Setup minimal properties (no credentials, using defaults)
        when(properties.getProperty("s3.endpoint", String.class))
                .thenReturn(Optional.of("https://s3.amazonaws.com"));
        when(properties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.of("test-object.desc"));
        when(properties.getProperty("s3.auth.access.key", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("s3.auth.secret.key", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("s3.region", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("s3.secure", Boolean.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("s3.auth.sts.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.empty());

        // Create configuration and client
        S3Configuration config = S3Configuration.fromProperties(properties);
        MinioClient client = MinioClientFactory.create(config);

        // Verify client was created successfully
        assertThat(client).isNotNull();
    }

    @Test
    void shouldCreateMinioClientWithInsecureEndpoint() {
        // Setup properties with insecure endpoint
        when(properties.getProperty("s3.endpoint", String.class))
                .thenReturn(Optional.of("https://localhost:9000"));
        when(properties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.of("test-object.desc"));
        when(properties.getProperty("s3.auth.access.key", String.class))
                .thenReturn(Optional.of("test-key"));
        when(properties.getProperty("s3.auth.secret.key", String.class))
                .thenReturn(Optional.of("test-secret"));
        when(properties.getProperty("s3.region", String.class))
                .thenReturn(Optional.of("us-east-1"));
        when(properties.getProperty("s3.secure", Boolean.class))
                .thenReturn(Optional.of(false));  // insecure
        when(properties.getProperty("s3.auth.sts.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.of(1800L));

        // Create configuration and client
        S3Configuration config = S3Configuration.fromProperties(properties);
        MinioClient client = MinioClientFactory.create(config);

        // Verify client was created successfully (should handle HTTPS->HTTP conversion)
        assertThat(client).isNotNull();
    }

    private void setupValidS3Properties() {
        when(properties.getProperty("s3.endpoint", String.class))
                .thenReturn(Optional.of("https://s3.amazonaws.com"));
        when(properties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.of("test-object.desc"));
        when(properties.getProperty("s3.auth.access.key", String.class))
                .thenReturn(Optional.of("test-access-key"));
        when(properties.getProperty("s3.auth.secret.key", String.class))
                .thenReturn(Optional.of("test-secret-key"));
        when(properties.getProperty("s3.region", String.class))
                .thenReturn(Optional.of("us-east-1"));
        when(properties.getProperty("s3.secure", Boolean.class))
                .thenReturn(Optional.of(true));
        when(properties.getProperty("s3.auth.sts.endpoint", String.class))
                .thenReturn(Optional.of("https://sts.amazonaws.com"));
        when(properties.getProperty("descriptor.value.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.of(3600L));
    }
}