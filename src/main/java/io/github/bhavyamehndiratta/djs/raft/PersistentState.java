package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.NodeId;

/**
 * State that must survive a crash: the current term and who this node voted
 * for in it. Without it, a restarted node could vote twice in the same term
 * and allow two leaders. Implementations must make a change durable before
 * {@link #setTermAndVote} returns, because the node replies right after.
 */
public interface PersistentState {

    long currentTerm();

    /** Null if this node has not voted in the current term. */
    NodeId votedFor();

    void setTermAndVote(long term, NodeId votedFor);
}
