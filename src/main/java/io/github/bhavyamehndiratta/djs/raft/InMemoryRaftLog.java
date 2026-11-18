package io.github.bhavyamehndiratta.djs.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * Test implementation. A simulated crash keeps this object and builds a new
 * node on top of it, which models a log that was safely on disk.
 */
public final class InMemoryRaftLog implements RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private long baseIndex = 0;
    private long baseTerm = 0;

    @Override
    public long lastIndex() {
        return baseIndex + entries.size();
    }

    @Override
    public long snapshotIndex() {
        return baseIndex;
    }

    @Override
    public long snapshotTerm() {
        return baseTerm;
    }

    @Override
    public long termAt(long index) {
        if (index == baseIndex) {
            return baseTerm;
        }
        return get(index).term();
    }

    @Override
    public LogEntry get(long index) {
        if (index <= baseIndex || index > lastIndex()) {
            throw new IllegalArgumentException("no entry at index " + index + ", log covers ("
                    + baseIndex + ", " + lastIndex() + "]");
        }
        return entries.get((int) (index - baseIndex - 1));
    }

    @Override
    public void append(LogEntry entry) {
        entries.add(entry);
    }

    @Override
    public void truncateFrom(long index) {
        if (index <= baseIndex || index > lastIndex()) {
            throw new IllegalArgumentException("cannot truncate from " + index + ", log covers ("
                    + baseIndex + ", " + lastIndex() + "]");
        }
        entries.subList((int) (index - baseIndex - 1), entries.size()).clear();
    }

    @Override
    public void compactTo(long index, long term) {
        if (index <= baseIndex) {
            throw new IllegalArgumentException("cannot compact to " + index + ", already compacted to " + baseIndex);
        }
        if (index <= lastIndex() && termAt(index) != term) {
            throw new IllegalArgumentException("term mismatch at " + index);
        }
        if (index >= lastIndex()) {
            entries.clear();
        } else {
            entries.subList(0, (int) (index - baseIndex)).clear();
        }
        baseIndex = index;
        baseTerm = term;
    }

    @Override
    public void reset(long index, long term) {
        if (index < 0) {
            throw new IllegalArgumentException("negative index");
        }
        entries.clear();
        baseIndex = index;
        baseTerm = term;
    }
}
