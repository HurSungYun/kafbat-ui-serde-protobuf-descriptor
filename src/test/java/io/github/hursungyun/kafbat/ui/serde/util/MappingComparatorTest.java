package io.github.hursungyun.kafbat.ui.serde.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.github.hursungyun.kafbat.ui.serde.util.MappingComparator.MappingDiff;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MappingComparatorTest {

    private Descriptors.Descriptor userDescriptor;
    private Descriptors.Descriptor orderDescriptor;
    private Descriptors.Descriptor addressDescriptor;

    @BeforeEach
    void setUp() throws Exception {
        // Load test descriptors from resource
        try (InputStream is = getClass().getResourceAsStream("/test_descriptors.desc")) {
            assertThat(is).isNotNull();
            DescriptorProtos.FileDescriptorSet descriptorSet =
                    DescriptorProtos.FileDescriptorSet.parseFrom(is);

            // Build descriptors
            Descriptors.FileDescriptor userFileDescriptor = null;
            Descriptors.FileDescriptor orderFileDescriptor = null;

            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                if (fileProto.getName().equals("user.proto")) {
                    userFileDescriptor =
                            Descriptors.FileDescriptor.buildFrom(
                                    fileProto, new Descriptors.FileDescriptor[0]);
                }
            }

            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                if (fileProto.getName().equals("order.proto")) {
                    orderFileDescriptor =
                            Descriptors.FileDescriptor.buildFrom(
                                    fileProto,
                                    new Descriptors.FileDescriptor[] {userFileDescriptor});
                }
            }

            assertThat(userFileDescriptor).isNotNull();
            assertThat(orderFileDescriptor).isNotNull();

            userDescriptor = userFileDescriptor.findMessageTypeByName("User");
            addressDescriptor = userFileDescriptor.findMessageTypeByName("Address");
            orderDescriptor = orderFileDescriptor.findMessageTypeByName("Order");

            assertThat(userDescriptor).isNotNull();
            assertThat(addressDescriptor).isNotNull();
            assertThat(orderDescriptor).isNotNull();
        }
    }

    @Test
    void shouldDetectNoChanges() {
        Map<String, Descriptors.Descriptor> oldMappings = new HashMap<>();
        oldMappings.put("user-topic", userDescriptor);
        oldMappings.put("order-topic", orderDescriptor);

        Map<String, Descriptors.Descriptor> newMappings = new HashMap<>();
        newMappings.put("user-topic", userDescriptor);
        newMappings.put("order-topic", orderDescriptor);

        MappingDiff diff = MappingComparator.compare(oldMappings, newMappings);

        assertThat(diff.hasChanges()).isFalse();
        assertThat(diff.getAdded()).isEmpty();
        assertThat(diff.getRemoved()).isEmpty();
        assertThat(diff.getChanged()).isEmpty();
        assertThat(diff.toString()).isEqualTo("No mapping changes");
    }

    @Test
    void shouldDetectAddedTopics() {
        Map<String, Descriptors.Descriptor> oldMappings = new HashMap<>();
        oldMappings.put("user-topic", userDescriptor);

        Map<String, Descriptors.Descriptor> newMappings = new HashMap<>();
        newMappings.put("user-topic", userDescriptor);
        newMappings.put("order-topic", orderDescriptor);
        newMappings.put("address-topic", addressDescriptor);

        MappingDiff diff = MappingComparator.compare(oldMappings, newMappings);

        assertThat(diff.hasChanges()).isTrue();
        assertThat(diff.getAdded()).hasSize(2);
        assertThat(diff.getAdded()).contains("order-topic", "address-topic");
        assertThat(diff.getRemoved()).isEmpty();
        assertThat(diff.getChanged()).isEmpty();
        assertThat(diff.toString()).contains("Added: ");
        assertThat(diff.toString()).contains("order-topic");
        assertThat(diff.toString()).contains("address-topic");
    }

    @Test
    void shouldDetectRemovedTopics() {
        Map<String, Descriptors.Descriptor> oldMappings = new HashMap<>();
        oldMappings.put("user-topic", userDescriptor);
        oldMappings.put("order-topic", orderDescriptor);
        oldMappings.put("address-topic", addressDescriptor);

        Map<String, Descriptors.Descriptor> newMappings = new HashMap<>();
        newMappings.put("user-topic", userDescriptor);

        MappingDiff diff = MappingComparator.compare(oldMappings, newMappings);

        assertThat(diff.hasChanges()).isTrue();
        assertThat(diff.getAdded()).isEmpty();
        assertThat(diff.getRemoved()).hasSize(2);
        assertThat(diff.getRemoved()).contains("order-topic", "address-topic");
        assertThat(diff.getChanged()).isEmpty();
        assertThat(diff.toString()).contains("Removed: ");
        assertThat(diff.toString()).contains("order-topic");
        assertThat(diff.toString()).contains("address-topic");
    }

    @Test
    void shouldDetectChangedTopics() {
        Map<String, Descriptors.Descriptor> oldMappings = new HashMap<>();
        oldMappings.put("topic-1", userDescriptor);
        oldMappings.put("topic-2", orderDescriptor);

        Map<String, Descriptors.Descriptor> newMappings = new HashMap<>();
        newMappings.put("topic-1", orderDescriptor); // Changed from User to Order
        newMappings.put("topic-2", userDescriptor); // Changed from Order to User

        MappingDiff diff = MappingComparator.compare(oldMappings, newMappings);

        assertThat(diff.hasChanges()).isTrue();
        assertThat(diff.getAdded()).isEmpty();
        assertThat(diff.getRemoved()).isEmpty();
        assertThat(diff.getChanged()).hasSize(2);
        assertThat(diff.getChanged()).containsKey("topic-1");
        assertThat(diff.getChanged()).containsKey("topic-2");

        String[] topic1Change = diff.getChanged().get("topic-1");
        assertThat(topic1Change[0]).isEqualTo(userDescriptor.getFullName());
        assertThat(topic1Change[1]).isEqualTo(orderDescriptor.getFullName());

        String[] topic2Change = diff.getChanged().get("topic-2");
        assertThat(topic2Change[0]).isEqualTo(orderDescriptor.getFullName());
        assertThat(topic2Change[1]).isEqualTo(userDescriptor.getFullName());

        assertThat(diff.toString()).contains("Changed: ");
        assertThat(diff.toString()).contains("topic-1");
        assertThat(diff.toString()).contains("topic-2");
        assertThat(diff.toString()).contains("->");
    }

    @Test
    void shouldDetectMultipleTypesOfChanges() {
        Map<String, Descriptors.Descriptor> oldMappings = new HashMap<>();
        oldMappings.put("existing-topic", userDescriptor);
        oldMappings.put("removed-topic", orderDescriptor);
        oldMappings.put("changed-topic", addressDescriptor);

        Map<String, Descriptors.Descriptor> newMappings = new HashMap<>();
        newMappings.put("existing-topic", userDescriptor); // No change
        newMappings.put("added-topic", orderDescriptor); // Added
        newMappings.put("changed-topic", userDescriptor); // Changed from Address to User

        MappingDiff diff = MappingComparator.compare(oldMappings, newMappings);

        assertThat(diff.hasChanges()).isTrue();
        assertThat(diff.getAdded()).hasSize(1);
        assertThat(diff.getAdded()).contains("added-topic");
        assertThat(diff.getRemoved()).hasSize(1);
        assertThat(diff.getRemoved()).contains("removed-topic");
        assertThat(diff.getChanged()).hasSize(1);
        assertThat(diff.getChanged()).containsKey("changed-topic");

        String toString = diff.toString();
        assertThat(toString).contains("Added: ");
        assertThat(toString).contains("Removed: ");
        assertThat(toString).contains("Changed: ");
    }

    @Test
    void shouldHandleEmptyOldMappings() {
        Map<String, Descriptors.Descriptor> oldMappings = new HashMap<>();

        Map<String, Descriptors.Descriptor> newMappings = new HashMap<>();
        newMappings.put("user-topic", userDescriptor);
        newMappings.put("order-topic", orderDescriptor);

        MappingDiff diff = MappingComparator.compare(oldMappings, newMappings);

        assertThat(diff.hasChanges()).isTrue();
        assertThat(diff.getAdded()).hasSize(2);
        assertThat(diff.getAdded()).contains("user-topic", "order-topic");
        assertThat(diff.getRemoved()).isEmpty();
        assertThat(diff.getChanged()).isEmpty();
    }

    @Test
    void shouldHandleEmptyNewMappings() {
        Map<String, Descriptors.Descriptor> oldMappings = new HashMap<>();
        oldMappings.put("user-topic", userDescriptor);
        oldMappings.put("order-topic", orderDescriptor);

        Map<String, Descriptors.Descriptor> newMappings = new HashMap<>();

        MappingDiff diff = MappingComparator.compare(oldMappings, newMappings);

        assertThat(diff.hasChanges()).isTrue();
        assertThat(diff.getAdded()).isEmpty();
        assertThat(diff.getRemoved()).hasSize(2);
        assertThat(diff.getRemoved()).contains("user-topic", "order-topic");
        assertThat(diff.getChanged()).isEmpty();
    }

    @Test
    void shouldHandleBothEmptyMappings() {
        Map<String, Descriptors.Descriptor> oldMappings = new HashMap<>();
        Map<String, Descriptors.Descriptor> newMappings = new HashMap<>();

        MappingDiff diff = MappingComparator.compare(oldMappings, newMappings);

        assertThat(diff.hasChanges()).isFalse();
        assertThat(diff.getAdded()).isEmpty();
        assertThat(diff.getRemoved()).isEmpty();
        assertThat(diff.getChanged()).isEmpty();
        assertThat(diff.toString()).isEqualTo("No mapping changes");
    }
}
