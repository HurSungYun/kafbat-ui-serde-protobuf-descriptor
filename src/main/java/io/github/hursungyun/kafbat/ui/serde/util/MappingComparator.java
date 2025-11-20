package io.github.hursungyun.kafbat.ui.serde.util;

import com.google.protobuf.Descriptors;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Simple utility to compare topic-to-message descriptor mappings. */
public class MappingComparator {

    /**
     * Compare old and new topic mappings to detect changes.
     *
     * @param oldMappings Previous topic mappings
     * @param newMappings Updated topic mappings
     * @return Comparison result showing added, removed, and changed topics
     */
    public static MappingDiff compare(
            Map<String, Descriptors.Descriptor> oldMappings,
            Map<String, Descriptors.Descriptor> newMappings) {

        Set<String> added = new HashSet<>();
        Set<String> removed = new HashSet<>();
        Map<String, String[]> changed = new HashMap<>();

        // Find added and changed topics
        for (Map.Entry<String, Descriptors.Descriptor> entry : newMappings.entrySet()) {
            String topic = entry.getKey();
            Descriptors.Descriptor newDescriptor = entry.getValue();

            if (!oldMappings.containsKey(topic)) {
                added.add(topic);
            } else {
                Descriptors.Descriptor oldDescriptor = oldMappings.get(topic);
                if (!oldDescriptor.getFullName().equals(newDescriptor.getFullName())) {
                    changed.put(
                            topic,
                            new String[] {
                                oldDescriptor.getFullName(), newDescriptor.getFullName()
                            });
                }
            }
        }

        // Find removed topics
        for (String topic : oldMappings.keySet()) {
            if (!newMappings.containsKey(topic)) {
                removed.add(topic);
            }
        }

        return new MappingDiff(added, removed, changed);
    }

    /** Result of comparing two mapping sets. */
    public static class MappingDiff {
        private final Set<String> added;
        private final Set<String> removed;
        private final Map<String, String[]> changed; // topic -> [old type, new type]

        public MappingDiff(Set<String> added, Set<String> removed, Map<String, String[]> changed) {
            this.added = added;
            this.removed = removed;
            this.changed = changed;
        }

        public Set<String> getAdded() {
            return added;
        }

        public Set<String> getRemoved() {
            return removed;
        }

        public Map<String, String[]> getChanged() {
            return changed;
        }

        public boolean hasChanges() {
            return !added.isEmpty() || !removed.isEmpty() || !changed.isEmpty();
        }

        @Override
        public String toString() {
            if (!hasChanges()) {
                return "No mapping changes";
            }

            StringBuilder sb = new StringBuilder();
            if (!added.isEmpty()) {
                sb.append("Added: ").append(added).append(" ");
            }
            if (!removed.isEmpty()) {
                sb.append("Removed: ").append(removed).append(" ");
            }
            if (!changed.isEmpty()) {
                sb.append("Changed: ");
                for (Map.Entry<String, String[]> entry : changed.entrySet()) {
                    sb.append(entry.getKey())
                            .append(" (")
                            .append(entry.getValue()[0])
                            .append(" -> ")
                            .append(entry.getValue()[1])
                            .append(") ");
                }
            }
            return sb.toString().trim();
        }
    }
}
