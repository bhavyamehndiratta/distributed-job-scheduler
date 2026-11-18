package io.github.bhavyamehndiratta.djs.raft;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Test state machine that supports snapshots. Its state is a hash chain over
 * every applied entry, so two nodes agree on the state exactly when they have
 * applied the same entries in the same order, however they got there (replay,
 * snapshot, or both). It also insists on seeing indexes without gaps.
 */
final class SnapshotRecorder implements StateMachine {

    private final BiConsumer<Long, LogEntry> onApply;
    private long applied = 0;
    private long hash = 0;

    SnapshotRecorder(BiConsumer<Long, LogEntry> onApply) {
        this.onApply = onApply;
    }

    private static long step(long hash, long index, LogEntry entry) {
        return hash * 1_000_003L + index * 31 + (entry.isNoop() ? 7 : entry.command().hashCode());
    }

    @Override
    public void apply(long index, LogEntry entry) {
        if (index != applied + 1) {
            throw new IllegalStateException("applied index " + index + " after " + applied);
        }
        onApply.accept(index, entry);
        hash = step(hash, index, entry);
        applied = index;
    }

    String state() {
        return applied + ":" + hash;
    }

    @Override
    public boolean supportsSnapshots() {
        return true;
    }

    @Override
    public String snapshot() {
        return state();
    }

    @Override
    public void restore(String snapshot) {
        String[] parts = snapshot.split(":");
        applied = Long.parseLong(parts[0]);
        hash = Long.parseLong(parts[1]);
    }

    /** The state after applying entries 1..upTo from the cluster-wide record of applied entries. */
    static String expected(Map<Long, LogEntry> appliedEntries, long upTo) {
        long h = 0;
        for (long i = 1; i <= upTo; i++) {
            LogEntry e = appliedEntries.get(i);
            if (e == null) {
                return "missing:" + i;
            }
            h = step(h, i, e);
        }
        return upTo + ":" + h;
    }
}
