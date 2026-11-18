package io.github.bhavyamehndiratta.djs.raft;

/**
 * Receives committed entries, in order, exactly once per node run. Must be
 * deterministic: every node applies the same entries and must end in the same
 * state. The scheduler is the real implementation; tests use recorders.
 *
 * <p>A state machine that supports snapshots can serialize its whole state and
 * load it back. {@link #restore} replaces all state, as if every entry up to
 * the snapshot had been applied.
 */
@FunctionalInterface
public interface StateMachine {
    StateMachine NONE = (index, entry) -> { };

    void apply(long index, LogEntry entry);

    default boolean supportsSnapshots() {
        return false;
    }

    default String snapshot() {
        throw new UnsupportedOperationException("this state machine does not support snapshots");
    }

    default void restore(String snapshot) {
        throw new UnsupportedOperationException("this state machine does not support snapshots");
    }
}
