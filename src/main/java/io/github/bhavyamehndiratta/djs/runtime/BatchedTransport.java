package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.net.Transport;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds a node's outgoing messages until {@link #flush()}. The driver flushes after it has fsynced the
 * log, so a follower never acknowledges, and a leader never replicates or answers a client, before the
 * entries behind that message are durable. Only the driver thread may use it.
 */
public final class BatchedTransport implements Transport {

    private record Out(NodeId from, NodeId to, Message message) {
    }

    private final Transport delegate;
    private final List<Out> queue = new ArrayList<>();

    public BatchedTransport(Transport delegate) {
        this.delegate = delegate;
    }

    @Override
    public void send(NodeId from, NodeId to, Message message) {
        queue.add(new Out(from, to, message));
    }

    /** Sends everything queued so far, in order. */
    public void flush() {
        if (queue.isEmpty()) {
            return;
        }
        List<Out> batch = new ArrayList<>(queue);
        queue.clear();
        for (Out o : batch) {
            delegate.send(o.from(), o.to(), o.message());
        }
    }
}
