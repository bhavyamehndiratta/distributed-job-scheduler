package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;

/**
 * Vote request. When {@code preVote} is true this is a PreVote probe: it asks
 * "would you vote for me in {@code term}?" without anyone changing their term.
 */
public record RequestVote(long term, NodeId candidate, long lastLogIndex, long lastLogTerm, boolean preVote)
        implements Message {
}
