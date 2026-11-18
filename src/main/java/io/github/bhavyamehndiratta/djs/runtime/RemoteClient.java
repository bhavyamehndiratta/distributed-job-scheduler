package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.scheduler.Client;
import io.github.bhavyamehndiratta.djs.scheduler.JobSpec;
import io.github.bhavyamehndiratta.djs.scheduler.JobView;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/** Blocking wrapper around {@link Client} for the CLI and tests. Throws IllegalStateException on timeout. */
public final class RemoteClient implements AutoCloseable {

    private final Driver driver;
    private final GrpcTransport transport;
    private final Client client;

    public RemoteClient(Map<NodeId, String> cluster, String host, long tickMillis, long timeoutMillis)
            throws IOException {
        NodeId id = new NodeId("client-" + UUID.randomUUID().toString().substring(0, 8));
        this.driver = new Driver(id.value(), tickMillis);
        this.transport = new GrpcTransport(id, host, 0, cluster, driver::postIncoming);
        this.client = new Client(id, new ArrayList<>(cluster.keySet()), transport, (int) (timeoutMillis / tickMillis));
        driver.setHandler(client::handle);
        driver.start(client::tick);
    }

    public Client.SubmitResult submit(String jobId, JobSpec spec, String idempotencyKey) {
        return await(() -> client.submit(jobId, spec, idempotencyKey));
    }

    public Client.CancelResult cancel(String jobId) {
        return await(() -> client.cancel(jobId));
    }

    public Client.JobInfo getJob(String jobId) {
        return await(() -> client.getJob(jobId));
    }

    public Client.MembersResult members(String action, String nodeId, String address) {
        return await(() -> client.members(action, nodeId, address));
    }

    public List<JobView> listJobs() {
        return await(client::listJobs);
    }

    private <T> T await(Supplier<Client.Call<T>> start) {
        Client.Call<T> call = driver.call(start, 5000);
        while (!driver.call(call::isDone, 5000)) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted");
            }
        }
        return driver.call(() -> {
            if (call.isTimedOut()) {
                throw new IllegalStateException("request timed out: no leader answered");
            }
            return call.value();
        }, 5000);
    }

    @Override
    public void close() {
        driver.close();
        transport.close();
    }
}
