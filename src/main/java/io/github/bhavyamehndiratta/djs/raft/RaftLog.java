package io.github.bhavyamehndiratta.djs.raft;

/**
 * The replicated log. Indexes start at 1; index 0 is a virtual entry with term
 * 0, so "the entry before the first one" always exists. Implementations must
 * make every change durable before returning, because a node acknowledges
 * entries right after calling them.
 *
 * <p>After a snapshot the front of the log is compacted away. Entries up to
 * {@link #snapshotIndex()} no longer exist, but the log still knows that
 * index's term ({@link #snapshotTerm()}), which is all the next append needs
 * for its consistency check. Indexes stay absolute: they never shift.
 */
public interface RaftLog {

    /** Index of the last entry, or the snapshot index if no entries follow it. */
    long lastIndex();

    /** Index of the last compacted entry, or 0 if the log was never compacted. */
    long snapshotIndex();

    /** Term of the entry at {@link #snapshotIndex()}, or 0 if never compacted. */
    long snapshotTerm();

    /**
     * Term of the entry at {@code index}, for index in [snapshotIndex, lastIndex]
     * (index 0 has term 0 when never compacted). Throws otherwise.
     */
    long termAt(long index);

    /** Entry at {@code index}, which must be in (snapshotIndex, lastIndex]. */
    LogEntry get(long index);

    void append(LogEntry entry);

    /** Removes the entry at {@code index} and everything after it. {@code index} must be above snapshotIndex. */
    void truncateFrom(long index);

    /**
     * Discards every entry up to and including {@code index} and makes it the
     * new snapshot point. Entries after it are kept. If the log has an entry at
     * {@code index} its term must equal {@code term}; if the log is shorter,
     * everything is discarded and the log ends at {@code index}.
     * {@code index} must be above the current snapshotIndex.
     */
    void compactTo(long index, long term);

    /** Discards the whole log and makes {@code index}/{@code term} its new base. */
    void reset(long index, long term);

    /**
     * Makes every change durable. A no-op for logs that fsync inside each call. A log that defers its
     * fsync (see {@code FileRaftLog.setDeferredSync}) relies on the caller to call this before any
     * message that depends on those changes leaves the node.
     */
    default void sync() {
    }

    default long lastTerm() {
        return termAt(lastIndex());
    }
}
