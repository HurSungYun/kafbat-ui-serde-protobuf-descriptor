package io.github.hursungyun.kafbat.ui.serde.sources;

import io.kafbat.ui.serde.api.PropertyResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class DescriptorSourceFactoryTest {

    private PropertyResolver properties;

    @BeforeEach
    void setUp() {
        properties = Mockito.mock(PropertyResolver.class);
    }

    @Test
    void shouldCreateLocalFileSourceWhenFilePathProvided() {
        when(properties.getProperty("descriptor.value.file", String.class))
                .thenReturn(Optional.of("/path/to/descriptors.desc"));
        when(properties.getProperty("descriptor.value.s3.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.empty());

        DescriptorSource source = DescriptorSourceFactory.create(properties);

        assertThat(source).isInstanceOf(LocalFileDescriptorSource.class);
        assertThat(source.getDescription()).isEqualTo("Local file: /path/to/descriptors.desc");
    }

    @Test
    void shouldCreateS3SourceWhenS3ConfigurationProvided() {
        // S3 configuration
        when(properties.getProperty("descriptor.value.s3.endpoint", String.class))
                .thenReturn(Optional.of("http://localhost:9000"));
        when(properties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.of("descriptors.desc"));
        when(properties.getProperty("descriptor.value.s3.access.key", String.class))
                .thenReturn(Optional.of("access-key"));
        when(properties.getProperty("descriptor.value.s3.secret.key", String.class))
                .thenReturn(Optional.of("secret-key"));
        
        // Optional properties with defaults
        when(properties.getProperty("descriptor.value.s3.region", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.secure", Boolean.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.empty());

        DescriptorSource source = DescriptorSourceFactory.create(properties);

        assertThat(source).isInstanceOf(S3DescriptorSource.class);
        assertThat(source.getDescription()).isEqualTo("S3: s3://test-bucket/descriptors.desc");
    }

    @Test
    void shouldPreferS3OverLocalFileWhenBothProvided() {
        // Both S3 and local file provided - should prefer S3
        when(properties.getProperty("descriptor.value.file", String.class))
                .thenReturn(Optional.of("/path/to/descriptors.desc"));
        when(properties.getProperty("descriptor.value.s3.endpoint", String.class))
                .thenReturn(Optional.of("http://localhost:9000"));
        when(properties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.of("descriptors.desc"));
        when(properties.getProperty("descriptor.value.s3.access.key", String.class))
                .thenReturn(Optional.of("access-key"));
        when(properties.getProperty("descriptor.value.s3.secret.key", String.class))
                .thenReturn(Optional.of("secret-key"));
        
        // Optional properties
        when(properties.getProperty("descriptor.value.s3.region", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.secure", Boolean.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.empty());

        DescriptorSource source = DescriptorSourceFactory.create(properties);

        assertThat(source).isInstanceOf(S3DescriptorSource.class);
    }

    @Test
    void shouldThrowExceptionWhenNoConfigurationProvided() {
        when(properties.getProperty("descriptor.value.file", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> DescriptorSourceFactory.create(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either descriptor.value.file or S3 configuration");
    }

    @Test
    void shouldCreateS3SourceWithoutCredentials() {
        // Test IAM role-based authentication (no access key/secret key)
        when(properties.getProperty("descriptor.value.s3.endpoint", String.class))
                .thenReturn(Optional.of("http://localhost:9000"));
        when(properties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.of("descriptors.desc"));
        when(properties.getProperty("descriptor.value.s3.access.key", String.class))
                .thenReturn(Optional.empty()); // Missing - should use IAM role
        when(properties.getProperty("descriptor.value.s3.secret.key", String.class))
                .thenReturn(Optional.empty()); // Missing - should use IAM role

        DescriptorSource source = DescriptorSourceFactory.create(properties);

        assertThat(source).isInstanceOf(S3DescriptorSource.class);
        assertThat(source.getDescription()).isEqualTo("S3: s3://test-bucket/descriptors.desc");
    }

    @Test
    void shouldCreateS3SourceWithCustomRefreshInterval() {
        when(properties.getProperty("descriptor.value.s3.endpoint", String.class))
                .thenReturn(Optional.of("http://localhost:9000"));
        when(properties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(properties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.of("descriptors.desc"));
        when(properties.getProperty("descriptor.value.s3.access.key", String.class))
                .thenReturn(Optional.of("access-key"));
        when(properties.getProperty("descriptor.value.s3.secret.key", String.class))
                .thenReturn(Optional.of("secret-key"));
        when(properties.getProperty("descriptor.value.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.of(120L)); // 2 minutes
        
        // Other optional properties
        when(properties.getProperty("descriptor.value.s3.region", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("descriptor.value.s3.secure", Boolean.class))
                .thenReturn(Optional.empty());

        DescriptorSource source = DescriptorSourceFactory.create(properties);

        assertThat(source).isInstanceOf(S3DescriptorSource.class);
        assertThat(source.supportsRefresh()).isTrue();
    }
}