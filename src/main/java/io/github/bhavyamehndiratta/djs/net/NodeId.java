package io.github.bhavyamehndiratta.djs.net;

/** Identity of a cluster node. A value type so it is safe as a map key. */
public record NodeId(String value) implements Comparable<NodeId> {

    public NodeId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
    }

    @Override
    public int compareTo(NodeId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
