package io.github.hursungyun.kafbat.ui.serde.auth;

import io.kafbat.ui.serde.api.PropertyResolver;
import io.minio.MinioClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3CredentialsProviderTest {

    @Mock
    private PropertyResolver properties;

    private MinioClient.Builder clientBuilder;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        clientBuilder = spy(MinioClient.builder().endpoint("http://test-endpoint"));
    }

    @Test
    void shouldConfigureWithExplicitCredentials() {
        when(properties.getProperty("protobuf.s3.access.key", String.class))
                .thenReturn(Optional.of("test-access-key"));
        when(properties.getProperty("protobuf.s3.secret.key", String.class))
                .thenReturn(Optional.of("test-secret-key"));
        when(properties.getProperty("protobuf.s3.sts.endpoint", String.class))
                .thenReturn(Optional.empty());

        S3CredentialsProvider.configure(clientBuilder, properties);

        verify(clientBuilder).credentials("test-access-key", "test-secret-key");
    }

    @Test
    void shouldUseCustomStsEndpoint() {
        when(properties.getProperty("protobuf.s3.access.key", String.class))
                .thenReturn(Optional.of("test-access-key"));
        when(properties.getProperty("protobuf.s3.secret.key", String.class))
                .thenReturn(Optional.of("test-secret-key"));
        when(properties.getProperty("protobuf.s3.sts.endpoint", String.class))
                .thenReturn(Optional.of("https://custom-sts.example.com"));

        S3CredentialsProvider.configure(clientBuilder, properties);

        verify(clientBuilder).credentials("test-access-key", "test-secret-key");
    }

    @Test
    void shouldHandleNoCredentialsGracefully() {
        // Test that the provider handles missing credentials without throwing exceptions
        // (Environment-specific tests would require integration testing)
        when(properties.getProperty("protobuf.s3.access.key", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("protobuf.s3.secret.key", String.class))
                .thenReturn(Optional.empty());
        when(properties.getProperty("protobuf.s3.sts.endpoint", String.class))
                .thenReturn(Optional.empty());

        // Should not throw exception during configuration
        // Actual credential resolution happens during S3 API calls
        S3CredentialsProvider.configure(clientBuilder, properties);
    }

    @Test
    void shouldConfigureWithDirectParameters() {
        Optional<String> accessKey = Optional.of("direct-access-key");
        Optional<String> secretKey = Optional.of("direct-secret-key");
        String stsEndpoint = "https://sts.amazonaws.com";

        S3CredentialsProvider.configureCredentials(clientBuilder, accessKey, secretKey, stsEndpoint);

        verify(clientBuilder).credentials("direct-access-key", "direct-secret-key");
    }
}