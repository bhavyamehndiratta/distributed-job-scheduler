package io.github.bhavyamehndiratta.djs.net;

/**
 * How a node sends messages to its peers. Sending is fire and forget: there is
 * no delivery guarantee, no ordering guarantee and no reply. The consensus code
 * must be correct under loss, delay, duplication and reordering, so it may rely
 * on nothing stronger. Two implementations exist: a real gRPC transport and
 * {@link SimulatedNetwork} for deterministic tests.
 */
public interface Transport {

    void send(NodeId from, NodeId to, Message message);
}
