package io.github.bhavyamehndiratta.djs.raft;

/**
 * Timing in ticks. {@code snapshotThresholdEntries} is how many applied entries
 * may pile up since the last snapshot before a node takes a new one; 0 turns
 * snapshots off.
 */
public record RaftConfig(int electionTimeoutTicks, int heartbeatIntervalTicks, int snapshotThresholdEntries) {

    public RaftConfig {
        if (heartbeatIntervalTicks < 1 || electionTimeoutTicks <= heartbeatIntervalTicks) {
            throw new IllegalArgumentException("need 1 <= heartbeatInterval < electionTimeout");
        }
        if (snapshotThresholdEntries < 0) {
            throw new IllegalArgumentException("snapshotThresholdEntries must not be negative");
        }
    }

    public RaftConfig(int electionTimeoutTicks, int heartbeatIntervalTicks) {
        this(electionTimeoutTicks, heartbeatIntervalTicks, 0);
    }

    public static RaftConfig defaults() {
        return new RaftConfig(10, 2, 0);
    }

    public RaftConfig withSnapshotThreshold(int entries) {
        return new RaftConfig(electionTimeoutTicks, heartbeatIntervalTicks, entries);
    }
}
