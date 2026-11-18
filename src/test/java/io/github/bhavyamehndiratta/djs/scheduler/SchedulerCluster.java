package io.github.bhavyamehndiratta.djs.scheduler;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.net.SimulatedNetwork;
import io.github.bhavyamehndiratta.djs.raft.InMemoryPersistentState;
import io.github.bhavyamehndiratta.djs.raft.InMemoryRaftLog;
import io.github.bhavyamehndiratta.djs.raft.InMemorySnapshotStore;
import io.github.bhavyamehndiratta.djs.raft.PersistentState;
import io.github.bhavyamehndiratta.djs.raft.RaftConfig;
import io.github.bhavyamehndiratta.djs.raft.RaftLog;
import io.github.bhavyamehndiratta.djs.raft.RaftObserver;
import io.github.bhavyamehndiratta.djs.raft.Role;
import io.github.bhavyamehndiratta.djs.raft.SnapshotStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;

/**
 * Scheduler nodes and workers on one simulated network, driven from a seed.
 * One tick of simulated time is {@link #TICK_MILLIS} of simulated wall clock.
 * Crashing a node keeps its persistent state and log (a disk that survived)
 * and rebuilds the scheduler state by replaying the log; crashing a worker
 * loses everything it was doing. The effect store is outside both, like the
 * real world the jobs act on.
 */
final class SchedulerCluster {

    static final long TICK_MILLIS = 100;
    static final long LEASE_TICKS = 30;

    final SimulatedNetwork net;
    final EffectStore store;
    private final long seed;
    private final List<NodeId> members = new ArrayList<>();
    private final List<NodeId> workerIds = new ArrayList<>();
    private final Map<NodeId, SchedulerNode> nodes = new TreeMap<>();
    private final Map<NodeId, PersistentState> disks = new HashMap<>();
    private final Map<NodeId, RaftLog> logs = new HashMap<>();
    private final Set<NodeId> upNodes = new TreeSet<>();
    private final Map<NodeId, Worker> workers = new TreeMap<>();
    private final Set<NodeId> upWorkers = new TreeSet<>();
    private final Map<Long, Set<NodeId>> leadersByTerm = new HashMap<>();
    private int restarts = 0;
    private final RaftConfig config;
    private final List<SchedulerNode> everCreated = new ArrayList<>();
    private final Map<NodeId, SnapshotStore> snapshotStores = new HashMap<>();
    private final List<Client> clients = new ArrayList<>();
    private final Map<NodeId, Long> stalledUntil = new HashMap<>();
    /** Command and outcome applied at each log index, first seen on any node. A later difference is a violation. */
    final Map<Long, String> appliedAt = new HashMap<>();
    final List<String> appliedViolations = new ArrayList<>();

    SchedulerCluster(int nodeCount, int workerCount, long seed, EffectStore store) {
        this(nodeCount, workerCount, seed, store, 0);
    }

    SchedulerCluster(int nodeCount, int workerCount, long seed, EffectStore store, int snapshotThreshold) {
        this.seed = seed;
        this.config = RaftConfig.defaults().withSnapshotThreshold(snapshotThreshold);
        this.store = store;
        this.net = new SimulatedNetwork(seed);
        for (int i = 1; i <= nodeCount; i++) {
            members.add(new NodeId("n" + i));
        }
        for (int i = 1; i <= workerCount; i++) {
            workerIds.add(new NodeId("w" + i));
        }
        for (NodeId id : members) {
            disks.put(id, new InMemoryPersistentState());
            logs.put(id, new InMemoryRaftLog());
            snapshotStores.put(id, new InMemorySnapshotStore());
            net.register(id, (from, msg) -> nodes.get(id).handle(from, msg));
            startNode(id);
        }
        for (NodeId id : workerIds) {
            net.register(id, (from, msg) -> workers.get(id).handle(from, msg));
            startWorker(id);
        }
    }

    private void startNode(NodeId id) {
        Random random = new Random(seed * 1_000_003L + id.hashCode() * 31L + restarts++);
        RaftObserver observer = (node, role, term) -> {
            if (role == Role.LEADER) {
                leadersByTerm.computeIfAbsent(term, t -> new TreeSet<>()).add(node);
            }
        };
        nodes.put(id, new SchedulerNode(id, members, net, random, config, disks.get(id),
                logs.get(id), this::nowMillis, LEASE_TICKS, observer, (index, command, outcome) -> {
                    String applied = command + " => " + outcome;
                    String earlier = appliedAt.putIfAbsent(index, applied);
                    if (earlier != null && !earlier.equals(applied)) {
                        appliedViolations.add("index " + index + " applied as [" + earlier + "] and [" + applied + "]");
                    }
                }, snapshotStores.get(id)));
        everCreated.add(nodes.get(id));
        upNodes.add(id);
        net.setNodeUp(id, true);
    }

    private void startWorker(NodeId id) {
        workers.put(id, new Worker(id, members, net, store, Handlers.builtIn()));
        upWorkers.add(id);
        net.setNodeUp(id, true);
    }

    Client addClient(String name, int timeoutTicks) {
        NodeId clientId = new NodeId(name);
        Client client = new Client(clientId, members, net, timeoutTicks);
        net.register(clientId, (from, msg) -> client.handle(from, msg));
        clients.add(client);
        return client;
    }

    long nowMillis() {
        return net.now() * TICK_MILLIS;
    }

    void tick() {
        net.step();
        for (NodeId id : new ArrayList<>(upNodes)) {
            if (stalledUntil.getOrDefault(id, 0L) > net.now()) {
                continue; // timers stalled: still receives messages, but never ticks
            }
            nodes.get(id).tick();
        }
        for (NodeId id : new ArrayList<>(upWorkers)) {
            workers.get(id).tick();
        }
        for (Client client : clients) {
            client.tick();
        }
    }

    void runFor(int ticks) {
        for (int i = 0; i < ticks; i++) {
            tick();
        }
    }

    boolean runUntil(BooleanSupplier condition, int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            if (condition.getAsBoolean()) {
                return true;
            }
            tick();
        }
        return condition.getAsBoolean();
    }

    /** The up node that believes it is leader in the highest term, or null. */
    SchedulerNode leader() {
        SchedulerNode best = null;
        for (NodeId id : upNodes) {
            SchedulerNode n = nodes.get(id);
            if (n.isLeader() && (best == null || n.raft().currentTerm() > best.raft().currentTerm())) {
                best = n;
            }
        }
        return best;
    }

    void awaitLeader() {
        if (!runUntil(() -> leader() != null, 1000)) {
            throw new AssertionError("no leader after 1000 ticks");
        }
    }

    /** Submits through the current leader and waits until the job shows up in its state. */
    void submit(String jobId, JobSpec spec, String idempotencyKey) {
        awaitLeader();
        if (leader().submitJob(jobId, spec, idempotencyKey) < 0) {
            throw new AssertionError("leader refused the submission");
        }
        if (!runUntil(() -> leader() != null && leader().stateMachine().job(jobId).isPresent(), 500)) {
            throw new AssertionError("job " + jobId + " was never applied");
        }
    }

    RunView runView(String runId) {
        SchedulerNode l = leader();
        return l == null ? null : l.stateMachine().run(runId).orElse(null);
    }

    RunState runState(String runId) {
        RunView v = runView(runId);
        return v == null ? null : v.state();
    }

    // ---- faults ----

    /**
     * Stops a node's timers for {@code ticks} ticks (a long GC pause or a slow clock) while it still receives
     * messages. A leader in this state never notices it lost its majority, which is the case ReadIndex exists for.
     */
    void stallNode(NodeId id, int ticks) {
        stalledUntil.put(id, net.now() + ticks);
    }

    void crashNode(NodeId id) {
        upNodes.remove(id);
        net.setNodeUp(id, false);
    }

    void restartNode(NodeId id) {
        startNode(id);
    }

    void crashWorker(NodeId id) {
        upWorkers.remove(id);
        net.setNodeUp(id, false);
    }

    void restartWorker(NodeId id) {
        startWorker(id);
    }

    void pauseWorker(NodeId id, int ticks) {
        workers.get(id).pause(ticks);
    }

    boolean isNodeUp(NodeId id) {
        return upNodes.contains(id);
    }

    List<NodeId> nodeIds() {
        return List.copyOf(members);
    }

    List<NodeId> workerIds() {
        return List.copyOf(workerIds);
    }

    int snapshotsTaken() {
        int total = 0;
        for (SchedulerNode n : everCreated) {
            total += n.raft().snapshotsTaken();
        }
        return total;
    }

    int snapshotInstalls() {
        int total = 0;
        for (SchedulerNode n : everCreated) {
            total += n.raft().snapshotsInstalled();
        }
        return total;
    }

    SchedulerNode node(NodeId id) {
        return nodes.get(id);
    }

    /** Digests of every up node's scheduler state; one entry means all replicas agree. */
    Set<String> distinctDigests() {
        Set<String> out = new TreeSet<>();
        for (NodeId id : upNodes) {
            out.add(nodes.get(id).stateMachine().digest());
        }
        return out;
    }

    Map<Long, Set<NodeId>> electionSafetyViolations() {
        Map<Long, Set<NodeId>> bad = new TreeMap<>();
        leadersByTerm.forEach((term, ids) -> {
            if (ids.size() > 1) {
                bad.put(term, ids);
            }
        });
        return bad;
    }
}
