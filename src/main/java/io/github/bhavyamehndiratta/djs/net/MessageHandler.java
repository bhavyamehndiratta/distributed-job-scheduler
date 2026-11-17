package io.github.bhavyamehndiratta.djs.net;

/** Receives messages addressed to one node. */
@FunctionalInterface
public interface MessageHandler {

    void onMessage(NodeId from, Message message);
}
