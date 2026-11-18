package io.github.bhavyamehndiratta.djs.raft;

/** Holds the latest snapshot. {@link #save} must be durable before it returns. */
public interface SnapshotStore {

    /** The latest snapshot, or null if none was ever saved. */
    Snapshot load();

    void save(Snapshot snapshot);
}
