package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.NodeId;

/**
 * Test implementation. A simulated crash keeps this object and builds a new
 * node on top of it, which models state that was safely on disk.
 */
public final class InMemoryPersistentState implements PersistentState {

    private long currentTerm = 0;
    private NodeId votedFor = null;

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public NodeId votedFor() {
        return votedFor;
    }

    @Override
    public void setTermAndVote(long term, NodeId votedFor) {
        if (term < currentTerm) {
            throw new IllegalStateException("term must never decrease: " + currentTerm + " -> " + term);
        }
        this.currentTerm = term;
        this.votedFor = votedFor;
    }
}
