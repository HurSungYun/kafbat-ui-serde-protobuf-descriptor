package io.github.hursungyun.kafbat.ui.serde.auth;

import io.kafbat.ui.serde.api.PropertyResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class S3ConfigurationTest {

    @Mock
    private PropertyResolver properties;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void shouldCreateConfigurationFromValidProperties() {
        // Setup valid S3 properties
        when(properties.getProperty("descriptor.s3.endpoint", String.class))
                .thenReturn(Optional.of("https://s3.amazonaws.com"));
        when(properties.getProperty("descriptor.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.s3.object.key", String.class))
                .thenReturn(Optional.of("test-object.desc"));
        when(properties.getProperty("descriptor.s3.access.key", String.class))
                .thenReturn(Optional.of("test-access-key"));
        when(properties.getProperty("descriptor.s3.secret.key", String.class))
                .thenReturn(Optional.of("test-secret-key"));
        when(properties.getProperty("descriptor.s3.region", String.class))
                .thenReturn(Optional.of("us-east-1"));
        when(properties.getProperty("descriptor.s3.secure", Boolean.class))
                .thenReturn(Optional.of(false));
        when(properties.getProperty("descriptor.s3.sts.endpoint", String.class))
                .thenReturn(Optional.of("https://sts.custom.com"));
        when(properties.getProperty("descriptor.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.of(1800L));

        S3Configuration config = S3Configuration.fromProperties(properties);

        assertThat(config.getEndpoint()).isEqualTo("https://s3.amazonaws.com");
        assertThat(config.getBucket()).isEqualTo("test-bucket");
        assertThat(config.getObjectKey()).isEqualTo("test-object.desc");
        assertThat(config.getAccessKey()).isPresent().contains("test-access-key");
        assertThat(config.getSecretKey()).isPresent().contains("test-secret-key");
        assertThat(config.getRegion()).isEqualTo("us-east-1");
        assertThat(config.isSecure()).isFalse();
        assertThat(config.getStsEndpoint()).isEqualTo("https://sts.custom.com");
        assertThat(config.getRefreshInterval()).isEqualTo(Duration.ofSeconds(1800));
    }

    @Test
    void shouldUseDefaultValuesForOptionalProperties() {
        // Setup minimal S3 properties
        when(properties.getProperty("descriptor.s3.endpoint", String.class))
                .thenReturn(Optional.of("https://s3.amazonaws.com"));
        when(properties.getProperty("descriptor.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.s3.object.key", String.class))
                .thenReturn(Optional.of("test-object.desc"));
        when(properties.getProperty("descriptor.s3.access.key", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.s3.secret.key", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.s3.region", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.s3.secure", Boolean.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.s3.sts.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.empty());

        S3Configuration config = S3Configuration.fromProperties(properties);

        assertThat(config.getEndpoint()).isEqualTo("https://s3.amazonaws.com");
        assertThat(config.getBucket()).isEqualTo("test-bucket");
        assertThat(config.getObjectKey()).isEqualTo("test-object.desc");
        assertThat(config.getAccessKey()).isEmpty();
        assertThat(config.getSecretKey()).isEmpty();
        assertThat(config.getRegion()).isNull();
        assertThat(config.isSecure()).isTrue(); // default
        assertThat(config.getStsEndpoint()).isEqualTo("https://sts.amazonaws.com"); // default
        assertThat(config.getRefreshInterval()).isEqualTo(Duration.ofHours(1)); // default
    }

    @Test
    void shouldThrowExceptionWhenRequiredPropertiesMissing() {
        // Missing endpoint
        when(properties.getProperty("descriptor.s3.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.s3.object.key", String.class))
                .thenReturn(Optional.of("test-object.desc"));

        assertThatThrownBy(() -> S3Configuration.fromProperties(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("descriptor.s3.endpoint is required");
    }

    @Test
    void shouldThrowExceptionWhenBucketMissing() {
        when(properties.getProperty("descriptor.s3.endpoint", String.class))
                .thenReturn(Optional.of("https://s3.amazonaws.com"));
        when(properties.getProperty("descriptor.s3.bucket", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.s3.object.key", String.class))
                .thenReturn(Optional.of("test-object.desc"));

        assertThatThrownBy(() -> S3Configuration.fromProperties(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("descriptor.s3.bucket is required");
    }

    @Test
    void shouldThrowExceptionWhenObjectKeyMissing() {
        when(properties.getProperty("descriptor.s3.endpoint", String.class))
                .thenReturn(Optional.of("https://s3.amazonaws.com"));
        when(properties.getProperty("descriptor.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.s3.object.key", String.class))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> S3Configuration.fromProperties(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("descriptor.s3.object.key is required");
    }

    @Test
    void shouldSupportCustomPrefix() {
        // Setup properties with custom prefix
        when(properties.getProperty("custom.prefix.endpoint", String.class))
                .thenReturn(Optional.of("https://custom.endpoint.com"));
        when(properties.getProperty("custom.prefix.bucket", String.class))
                .thenReturn(Optional.of("custom-bucket"));
        when(properties.getProperty("custom.prefix.object.key", String.class))
                .thenReturn(Optional.of("custom-object.json"));
        when(properties.getProperty("custom.prefix.access.key", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("custom.prefix.secret.key", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("custom.prefix.region", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("custom.prefix.secure", Boolean.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("custom.prefix.sts.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("custom.prefix.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.empty());

        S3Configuration config = S3Configuration.fromProperties(properties, "custom.prefix");

        assertThat(config.getEndpoint()).isEqualTo("https://custom.endpoint.com");
        assertThat(config.getBucket()).isEqualTo("custom-bucket");
        assertThat(config.getObjectKey()).isEqualTo("custom-object.json");
    }
}