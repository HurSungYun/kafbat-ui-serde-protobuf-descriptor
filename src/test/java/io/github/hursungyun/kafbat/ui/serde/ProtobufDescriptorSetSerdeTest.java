package io.github.hursungyun.kafbat.ui.serde;

import io.kafbat.ui.serde.api.PropertyResolver;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class ProtobufDescriptorSetSerdeTest {

    @Test
    void shouldThrowExceptionWhenDescriptorSetFilePropertyIsMissing() {
        ProtobufDescriptorSetSerde serde = new ProtobufDescriptorSetSerde();
        PropertyResolver serdeProperties = Mockito.mock(PropertyResolver.class);
        PropertyResolver clusterProperties = Mockito.mock(PropertyResolver.class);
        PropertyResolver appProperties = Mockito.mock(PropertyResolver.class);

        when(serdeProperties.getProperty("protobuf.descriptor.set.file", String.class))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> serde.configure(serdeProperties, clusterProperties, appProperties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("protobuf.descriptor.set.file property is required");
    }

    @Test
    void shouldHaveDescription() {
        ProtobufDescriptorSetSerde serde = new ProtobufDescriptorSetSerde();
        
        assertThat(serde.getDescription())
                .isPresent()
                .contains("Protobuf Descriptor Set Serde - deserializes protobuf messages using descriptor set file");
    }

    @Test
    void shouldSupportDeserializationAndSerialization() {
        ProtobufDescriptorSetSerde serde = new ProtobufDescriptorSetSerde();
        
        assertThat(serde.canDeserialize("test-topic", null)).isTrue();
        assertThat(serde.canSerialize("test-topic", null)).isTrue();
    }
}