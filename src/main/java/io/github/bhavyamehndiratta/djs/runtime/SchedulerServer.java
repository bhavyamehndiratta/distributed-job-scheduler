package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.raft.FilePersistentState;
import io.github.bhavyamehndiratta.djs.raft.FileRaftLog;
import io.github.bhavyamehndiratta.djs.raft.FileSnapshotStore;
import io.github.bhavyamehndiratta.djs.raft.RaftConfig;
import io.github.bhavyamehndiratta.djs.raft.RaftObserver;
import io.github.bhavyamehndiratta.djs.scheduler.SchedulerNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;

/** One cluster member as a real process: gRPC transport, files on disk, wall clock, one driver thread. */
public final class SchedulerServer implements AutoCloseable {

    private static final int SNAPSHOT_THRESHOLD = 1000;

    private final NodeId id;
    private final Driver driver;
    private final GrpcTransport transport;
    private final FileRaftLog log;
    private final SchedulerNode node;
    private final BatchedTransport outbox;

    public SchedulerServer(NodeId id, Map<NodeId, String> cluster, Path dataDir, String host, long tickMillis,
                           long leaseTicks) throws IOException {
        this(id, cluster, dataDir, host, tickMillis, leaseTicks, false);
    }

    /** {@code joining} starts a node that is being added to a running cluster, see {@code djs member add}. */
    public SchedulerServer(NodeId id, Map<NodeId, String> cluster, Path dataDir, String host, long tickMillis,
                           long leaseTicks, boolean joining) throws IOException {
        this.id = id;
        String own = cluster.get(id);
        if (own == null) {
            throw new IllegalArgumentException(id.value() + " is not in the cluster list");
        }
        Files.createDirectories(dataDir);
        this.driver = new Driver(id.value(), tickMillis);
        this.transport = new GrpcTransport(id, host, ClusterSpec.port(own), cluster, driver::postIncoming);
        this.log = new FileRaftLog(dataDir.resolve("raft.log"));
        this.log.setDeferredSync(true); // group commit: one fsync per batch, see Driver.setAfterBatch
        this.outbox = new BatchedTransport(transport);
        this.node = new SchedulerNode(id, cluster.keySet(), outbox, new Random(), RaftConfig.defaults().withSnapshotThreshold(SNAPSHOT_THRESHOLD),
                new FilePersistentState(dataDir.resolve("raft.state")), log, System::currentTimeMillis, leaseTicks,
                RaftObserver.NONE, (index, command, outcome) -> { },
                new FileSnapshotStore(dataDir.resolve("raft.snapshot")), joining, transport::addPeer);
        driver.setHandler(node::handle);
        // Nothing leaves this node until the log is on disk: fsync once per batch, then release the messages.
        driver.setAfterBatch(() -> {
            log.sync();
            outbox.flush();
        });
        driver.start(node::tick);
    }

    public NodeId id() {
        return id;
    }

    public String address() {
        return transport.address();
    }

    public boolean isLeader() {
        return driver.call(node::isLeader, 5000);
    }

    @Override
    public void close() {
        driver.close();
        transport.close();
        log.close();
    }
}
