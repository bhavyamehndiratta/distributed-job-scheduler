package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.scheduler.EffectStore;
import io.github.bhavyamehndiratta.djs.scheduler.Handler;
import io.github.bhavyamehndiratta.djs.scheduler.Handlers;
import io.github.bhavyamehndiratta.djs.scheduler.Worker;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/** A worker as a real process. The effect store here is in memory and stands in for the outside world. */
public final class WorkerProcess implements AutoCloseable {

    private final Driver driver;
    private final GrpcTransport transport;
    private final EffectStore store;

    public WorkerProcess(NodeId id, Map<NodeId, String> cluster, String host, int port, long tickMillis,
                         EffectStore store) throws IOException {
        this(id, cluster, host, port, tickMillis, store, Handlers.builtIn());
    }

    public WorkerProcess(NodeId id, Map<NodeId, String> cluster, String host, int port, long tickMillis,
                         EffectStore store, Map<String, Handler> handlers) throws IOException {
        this.store = store;
        this.driver = new Driver(id.value(), tickMillis);
        this.transport = new GrpcTransport(id, host, port, cluster, driver::postIncoming);
        Worker worker = new Worker(id, new ArrayList<>(cluster.keySet()), transport, store, handlers);
        driver.setHandler(worker::handle);
        driver.start(worker::tick);
    }

    public int effectCount(String runId) {
        return driver.call(() -> store.effectCount(runId), 5000);
    }

    public int totalEffects() {
        return driver.call(store::totalEffects, 5000);
    }

    @Override
    public void close() {
        driver.close();
        transport.close();
    }
}
