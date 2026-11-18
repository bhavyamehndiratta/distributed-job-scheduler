package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.Message;

public record RequestVoteResponse(long term, boolean granted, boolean preVote) implements Message {
}
