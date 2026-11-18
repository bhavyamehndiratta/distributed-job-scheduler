package io.github.bhavyamehndiratta.djs.raft;

/** Test implementation. A simulated crash keeps this object, like a snapshot file that survived. */
public final class InMemorySnapshotStore implements SnapshotStore {

    private Snapshot latest = null;

    @Override
    public Snapshot load() {
        return latest;
    }

    @Override
    public void save(Snapshot snapshot) {
        latest = snapshot;
    }
}
