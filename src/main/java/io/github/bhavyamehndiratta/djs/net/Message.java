package io.github.bhavyamehndiratta.djs.net;

/**
 * Marker for anything sent between nodes. Concrete Raft and scheduler messages
 * implement this. Messages should be immutable, because the simulated network
 * may deliver the same instance more than once.
 */
public interface Message {
}
