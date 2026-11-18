package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.net.SimulatedNetwork;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A whole cluster inside the simulated network, driven tick by tick from one
 * seed. Crashing a node stops it and cuts it off; restarting builds a fresh
 * node on the same persistent state and log, like a process restarting from
 * disk. The cluster also acts as a checker: it records every leader per term
 * and every entry any node applies, so tests can assert the Raft safety
 * properties directly.
 */
final class TestCluster {

    final SimulatedNetwork net;
    private final long seed;
    private final List<NodeId> members = new ArrayList<>();
    private final List<NodeId> initialMembers = new ArrayList<>();
    private final Map<NodeId, RaftNode> nodes = new TreeMap<>();
    private final Map<NodeId, PersistentState> disks = new HashMap<>();
    private final Map<NodeId, RaftLog> logs = new HashMap<>();
    private final Set<NodeId> up = new TreeSet<>();
    private final Map<Long, Set<NodeId>> leadersByTerm = new HashMap<>();
    private final Map<Long, LogEntry> applied = new TreeMap<>();
    private final List<String> stateMachineViolations = new ArrayList<>();
    private int restarts = 0;
    private final RaftConfig config;
    private final Map<NodeId, SnapshotRecorder> recorders = new HashMap<>();
    private final Map<NodeId, SnapshotStore> snapshotStores = new HashMap<>();
    private final List<RaftNode> everCreated = new ArrayList<>();

    TestCluster(int size, long seed) {
        this(size, seed, 0);
    }

    TestCluster(int size, long seed, int snapshotThreshold) {
        this.seed = seed;
        this.config = RaftConfig.defaults().withSnapshotThreshold(snapshotThreshold);
        this.net = new SimulatedNetwork(seed);
        for (int i = 1; i <= size; i++) {
            members.add(new NodeId("n" + i));
        }
        initialMembers.addAll(members);
        for (NodeId id : members) {
            disks.put(id, new InMemoryPersistentState());
            logs.put(id, new InMemoryRaftLog());
            snapshotStores.put(id, new InMemorySnapshotStore());
            net.register(id, (from, msg) -> nodes.get(id).handle(from, msg));
            start(id);
        }
    }

    private void start(NodeId id) {
        Random random = new Random(seed * 1_000_003L + id.hashCode() * 31L + restarts++);
        RaftObserver observer = (node, role, term) -> {
            if (role == Role.LEADER) {
                leadersByTerm.computeIfAbsent(term, t -> new TreeSet<>()).add(node);
            }
        };
        SnapshotRecorder recorder = new SnapshotRecorder((index, entry) -> {
            LogEntry earlier = applied.putIfAbsent(index, entry);
            if (earlier != null && !earlier.equals(entry)) {
                stateMachineViolations.add("index " + index + ": " + earlier + " vs " + entry + " (applied by " + id + ")");
            }
        });
        recorders.put(id, recorder);
        if (initialMembers.contains(id)) {
            nodes.put(id, new RaftNode(id, initialMembers, net, random, config,
                    disks.get(id), logs.get(id), recorder, snapshotStores.get(id), observer));
        } else {
            nodes.put(id, RaftNode.joining(id, net, random, config,
                    disks.get(id), logs.get(id), recorder, snapshotStores.get(id), observer));
        }
        everCreated.add(nodes.get(id));
        up.add(id);
        net.setNodeUp(id, true);
    }

    void tick() {
        net.step();
        for (NodeId id : up) {
            nodes.get(id).tick();
        }
    }

    void runFor(int ticks) {
        for (int i = 0; i < ticks; i++) {
            tick();
        }
    }

    /** Starts a new node that knows no configuration yet. It joins once a leader adds it. */
    void addNode(NodeId id) {
        members.add(id);
        disks.put(id, new InMemoryPersistentState());
        logs.put(id, new InMemoryRaftLog());
        snapshotStores.put(id, new InMemorySnapshotStore());
        net.register(id, (from, msg) -> nodes.get(id).handle(from, msg));
        start(id);
    }

    void crash(NodeId id) {
        up.remove(id);
        net.setNodeUp(id, false);
    }

    void restart(NodeId id) {
        start(id);
    }

    boolean isUp(NodeId id) {
        return up.contains(id);
    }

    List<NodeId> upNodes() {
        return new ArrayList<>(up);
    }

    RaftNode node(NodeId id) {
        return nodes.get(id);
    }

    RaftLog log(NodeId id) {
        return logs.get(id);
    }

    List<NodeId> members() {
        return List.copyOf(members);
    }

    /** Up nodes that currently believe they are leader. */
    List<RaftNode> leaders() {
        List<RaftNode> out = new ArrayList<>();
        for (NodeId id : up) {
            if (nodes.get(id).role() == Role.LEADER) {
                out.add(nodes.get(id));
            }
        }
        return out;
    }

    /** Proposes on the first up node that believes it is leader. Returns its index, or -1 if none. */
    long propose(String command) {
        List<RaftNode> leaders = leaders();
        return leaders.isEmpty() ? -1 : leaders.get(0).propose(command);
    }

    /** Runs until exactly one up node is leader, or fails after maxTicks. */
    RaftNode awaitSingleLeader(int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            tick();
            List<RaftNode> leaders = leaders();
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
        }
        throw new AssertionError("no single leader after " + maxTicks + " ticks");
    }

    /** Every entry any node has ever applied, by index. These are the "acknowledged" entries. */
    Map<Long, LogEntry> appliedEntries() {
        return new TreeMap<>(applied);
    }

    /** The state machine's state on a node, as a short string. */
    String stateOf(NodeId id) {
        return recorders.get(id).state();
    }

    int snapshotsTaken() {
        int total = 0;
        for (RaftNode n : everCreated) {
            total += n.snapshotsTaken();
        }
        return total;
    }

    int snapshotInstalls() {
        int total = 0;
        for (RaftNode n : everCreated) {
            total += n.snapshotsInstalled();
        }
        return total;
    }

    /** Number of distinct terms in which some node became leader. */
    int electedTerms() {
        return leadersByTerm.size();
    }

    /** Terms in which more than one node became leader. Must always be empty. */
    Map<Long, Set<NodeId>> electionSafetyViolations() {
        Map<Long, Set<NodeId>> bad = new TreeMap<>();
        leadersByTerm.forEach((term, ids) -> {
            if (ids.size() > 1) {
                bad.put(term, ids);
            }
        });
        return bad;
    }

    /** Two nodes applying different entries at the same index. Must always be empty. */
    List<String> stateMachineViolations() {
        return List.copyOf(stateMachineViolations);
    }

    /**
     * Log Matching: if two logs hold an entry with the same index and term, the
     * logs are identical up to that index. Must always be empty.
     */
    List<String> logMatchingViolations() {
        List<String> bad = new ArrayList<>();
        List<NodeId> ids = new ArrayList<>(members);
        for (int a = 0; a < ids.size(); a++) {
            for (int b = a + 1; b < ids.size(); b++) {
                RaftLog la = logs.get(ids.get(a));
                RaftLog lb = logs.get(ids.get(b));
                long common = Math.min(la.lastIndex(), lb.lastIndex());
                long floor = Math.max(la.snapshotIndex(), lb.snapshotIndex());
                for (long i = common; i >= floor; i--) {
                    if (la.termAt(i) == lb.termAt(i)) {
                        for (long j = floor + 1; j <= i; j++) {
                            if (!la.get(j).equals(lb.get(j))) {
                                bad.add(ids.get(a) + " vs " + ids.get(b) + " differ at " + j + " but agree at " + i);
                            }
                        }
                        break;
                    }
                }
            }
        }
        return bad;
    }
}
