package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.Objects;

/** Sent by a leader to a follower that is so far behind that the entries it needs were compacted away. */
public record InstallSnapshot(long term, NodeId leader, long lastIncludedIndex, long lastIncludedTerm,
                              String data) implements Message {
    public InstallSnapshot {
        Objects.requireNonNull(data);
    }
}
