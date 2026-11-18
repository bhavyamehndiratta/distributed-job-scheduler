package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.Message;

/** {@code matchIndex} is how far the follower is now known to match the leader's log. */
public record InstallSnapshotResponse(long term, long matchIndex) implements Message {
}
