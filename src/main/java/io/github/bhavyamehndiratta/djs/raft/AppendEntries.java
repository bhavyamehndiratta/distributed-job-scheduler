package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.List;

/**
 * Leader to follower. Carries the entries that follow {@code prevLogIndex}
 * (empty for a pure heartbeat) and the leader's commit index. {@code readSeq}
 * is the leader's latest ReadIndex sequence number; the follower echoes it in its
 * response so the leader can tell the answer was produced after a read began.
 */
public record AppendEntries(long term, NodeId leader, long prevLogIndex, long prevLogTerm,
                            List<LogEntry> entries, long leaderCommit, long readSeq) implements Message {

    public AppendEntries {
        entries = List.copyOf(entries);
    }

    public AppendEntries(long term, NodeId leader, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) {
        this(term, leader, prevLogIndex, prevLogTerm, entries, leaderCommit, 0);
    }

    /** A bare heartbeat that asserts nothing about the log. Mostly useful in tests. */
    public AppendEntries(long term, NodeId leader) {
        this(term, leader, 0, 0, List.of(), 0, 0);
    }
}
