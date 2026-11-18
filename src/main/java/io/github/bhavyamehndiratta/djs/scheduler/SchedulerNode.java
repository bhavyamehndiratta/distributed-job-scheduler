package io.github.bhavyamehndiratta.djs.scheduler;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.net.Transport;
import io.github.bhavyamehndiratta.djs.raft.ClusterConfig;
import io.github.bhavyamehndiratta.djs.raft.InMemorySnapshotStore;
import io.github.bhavyamehndiratta.djs.raft.PersistentState;
import io.github.bhavyamehndiratta.djs.raft.RaftConfig;
import io.github.bhavyamehndiratta.djs.raft.RaftLog;
import io.github.bhavyamehndiratta.djs.raft.RaftNode;
import io.github.bhavyamehndiratta.djs.raft.RaftObserver;
import io.github.bhavyamehndiratta.djs.raft.Role;
import io.github.bhavyamehndiratta.djs.raft.SnapshotStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * One cluster member: a Raft node plus the scheduler on top of it. Every node
 * applies the committed log to its own {@link SchedulerStateMachine}. Only the
 * current leader also runs the scheduling service, which is deliberately thin
 * and keeps nothing that cannot be rebuilt from the replicated state:
 * <ul>
 *   <li>proposes FireRun for due jobs (the state machine dedupes repeats),</li>
 *   <li>answers worker polls with AssignRun, whose log index becomes the fencing token,</li>
 *   <li>tracks heartbeat leases in memory and proposes ExpireAssignment when one runs out.</li>
 * </ul>
 * A new leader starts every lease from zero, because it cannot know how much of
 * the old leader's countdown had already elapsed.
 *
 * <p>Wall-clock time comes from the supplied clock and is only ever copied into
 * log entries; the state machine itself never reads a clock.
 */
public final class SchedulerNode {

    private static final int HOUSEKEEPING_TICKS = 2;
    private static final int PROPOSAL_RETRY_TICKS = 40;

    private record Pending(NodeId replyTo, Command command) {
    }

    private static final class Lease {
        final long token;
        final long leaseTicks;
        long lastSeenTick;

        Lease(long token, long leaseTicks, long lastSeenTick) {
            this.token = token;
            this.leaseTicks = leaseTicks;
            this.lastSeenTick = lastSeenTick;
        }
    }

    private final NodeId id;
    private final Transport transport;
    private final LongSupplier clockMillis;
    private final long leaseTicks;
    private final SchedulerStateMachine stateMachine;
    private final RaftNode raft;

    private long tick = 0;
    private long leaderKey = -1;
    private final Map<Long, Pending> pending = new HashMap<>();

    private record ClientPending(NodeId replyTo, long requestId, Command command) {
    }

    private final Map<Long, ClientPending> clientPending = new HashMap<>();
    private final Map<Long, ClientPending> memberPending = new HashMap<>();
    private record PendingRead(NodeId from, Message request) {
    }

    private final Map<Long, PendingRead> readPending = new HashMap<>();
    private long nextReadId = 1;
    private final BiConsumer<NodeId, String> peerAddresses;
    private ClusterConfig syncedConfig = null;
    private final Map<String, Lease> leases = new HashMap<>();
    private final Map<String, Long> recentProposals = new HashMap<>();
    private final Set<String> assigningWorkers = new HashSet<>();
    private final Set<String> assigningRuns = new HashSet<>();

    public SchedulerNode(NodeId id, Collection<NodeId> members, Transport transport, Random random,
                         RaftConfig config, PersistentState state, RaftLog log, LongSupplier clockMillis,
                         long leaseTicks, RaftObserver observer, SchedulerStateMachine.Listener listener) {
        this(id, members, transport, random, config, state, log, clockMillis, leaseTicks, observer, listener,
                new InMemorySnapshotStore());
    }

    public SchedulerNode(NodeId id, Collection<NodeId> members, Transport transport, Random random,
                         RaftConfig config, PersistentState state, RaftLog log, LongSupplier clockMillis,
                         long leaseTicks, RaftObserver observer, SchedulerStateMachine.Listener listener,
                         SnapshotStore snapshots) {
        this(id, members, transport, random, config, state, log, clockMillis, leaseTicks, observer, listener,
                snapshots, false, (peer, address) -> { });
    }

    /**
     * {@code joining} starts a node that is being added to a running cluster: it knows no configuration
     * and learns it from its leader. {@code peerAddresses} is told the address of every node named in the
     * configuration, so the transport can reach nodes that were added after it started.
     */
    public SchedulerNode(NodeId id, Collection<NodeId> members, Transport transport, Random random,
                         RaftConfig config, PersistentState state, RaftLog log, LongSupplier clockMillis,
                         long leaseTicks, RaftObserver observer, SchedulerStateMachine.Listener listener,
                         SnapshotStore snapshots, boolean joining, BiConsumer<NodeId, String> peerAddresses) {
        if (leaseTicks < 3) {
            throw new IllegalArgumentException("leaseTicks must be at least 3");
        }
        this.id = id;
        this.transport = transport;
        this.clockMillis = clockMillis;
        this.leaseTicks = leaseTicks;
        this.stateMachine = new SchedulerStateMachine(SchedulerStateMachine.DEFAULT_IDEMPOTENCY_RETENTION_MILLIS,
                (index, command, outcome) -> {
                    onApplied(index, command, outcome);
                    listener.applied(index, command, outcome);
                });
        this.peerAddresses = peerAddresses;
        this.raft = joining
                ? RaftNode.joining(id, transport, random, config, state, log, stateMachine, snapshots, observer)
                : new RaftNode(id, members, transport, random, config, state, log, stateMachine, snapshots,
                        observer);
        syncAddresses();
    }

    public NodeId id() {
        return id;
    }

    public RaftNode raft() {
        return raft;
    }

    public SchedulerStateMachine stateMachine() {
        return stateMachine;
    }

    public boolean isLeader() {
        return raft.role() == Role.LEADER;
    }

    // ---- client entry points (Stage 8 puts a network client in front of these) ----

    /** Proposes a job submission. Returns its log index, or -1 if this node is not the leader. */
    public long submitJob(String jobId, JobSpec spec, String idempotencyKey) {
        return raft.propose(CommandCodec.encode(
                new Command.SubmitJob(jobId, spec, idempotencyKey, clockMillis.getAsLong())));
    }

    public long cancelJob(String jobId) {
        return raft.propose(CommandCodec.encode(new Command.CancelJob(jobId)));
    }

    // ---- inputs ----

    public void tick() {
        tick++;
        raft.tick();
        syncAddresses();
        long key = isLeader() ? raft.currentTerm() : -1;
        if (key != leaderKey) {
            leaderKey = key;
            pending.clear();
            clientPending.clear();
            memberPending.clear();
            readPending.clear();
            leases.clear();
            recentProposals.clear();
            assigningWorkers.clear();
            assigningRuns.clear();
        }
        if (isLeader()) {
            replyToCommittedMemberChanges();
            serveReads();
        }
        if (isLeader() && tick % HOUSEKEEPING_TICKS == 0) {
            housekeeping();
        }
    }

    public void handle(NodeId from, Message message) {
        switch (message) {
            case WorkerMessages.Poll m -> whenLeader(from, () -> onPoll(from, m));
            case WorkerMessages.Heartbeat m -> whenLeader(from, () -> onHeartbeat(from, m));
            case WorkerMessages.Started m -> whenLeader(from, () -> onStarted(m));
            case WorkerMessages.Completed m -> whenLeader(from, () -> onCompleted(from, m));
            case ClientMessages.Submit m -> whenLeader(from, () -> onClientSubmit(from, m));
            case ClientMessages.Cancel m -> whenLeader(from, () -> onClientCancel(from, m));
            case ClientMessages.GetJob m -> whenLeader(from, () -> onClientGetJob(from, m));
            case ClientMessages.Members m -> whenLeader(from, () -> onMembers(from, m));
            case ClientMessages.ListJobs m -> whenLeader(from, () -> startRead(from, m));
            default -> raft.handle(from, message);
        }
        serveReads();
        syncAddresses();
    }

    private void whenLeader(NodeId from, Runnable action) {
        if (isLeader()) {
            action.run();
        } else {
            transport.send(id, from, new WorkerMessages.NotLeader(raft.leaderId()));
        }
    }

    // ---- leader housekeeping ----

    private void housekeeping() {
        recentProposals.values().removeIf(at -> tick - at > PROPOSAL_RETRY_TICKS);

        for (Command.FireRun fire : stateMachine.dueFires(clockMillis.getAsLong())) {
            proposeOnce("F:" + fire.jobId() + "@" + fire.scheduledTime(), fire);
        }

        Set<String> active = new HashSet<>();
        for (RunView run : stateMachine.runs()) {
            if (run.state() != RunState.ASSIGNED && run.state() != RunState.RUNNING) {
                continue;
            }
            active.add(run.runId());
            Lease lease = leases.get(run.runId());
            if (lease == null || lease.token != run.token()) {
                // First time this leader sees the assignment: its countdown starts now.
                leases.put(run.runId(), new Lease(run.token(), run.leaseTicks(), tick));
            } else if (tick - lease.lastSeenTick > lease.leaseTicks) {
                proposeOnce("X:" + run.runId() + ":" + run.token(),
                        new Command.ExpireAssignment(run.runId(), run.token()));
            }
        }
        leases.keySet().retainAll(active);
    }

    private long proposeOnce(String key, Command command) {
        if (recentProposals.containsKey(key)) {
            return -1;
        }
        long index = raft.propose(CommandCodec.encode(command));
        if (index > 0) {
            recentProposals.put(key, tick);
        }
        return index;
    }

    // ---- worker messages (leader only) ----

    private void onPoll(NodeId from, WorkerMessages.Poll poll) {
        String worker = poll.workerId();
        // A run already assigned to this worker: the earlier Assignment message may have been lost, so resend it.
        for (RunView run : stateMachine.runsInState(RunState.ASSIGNED)) {
            if (worker.equals(run.workerId())) {
                sendAssignment(from, run);
                return;
            }
        }
        if (assigningWorkers.contains(worker)) {
            return;
        }
        for (RunView run : stateMachine.runsInState(RunState.SCHEDULED)) {
            if (assigningRuns.contains(run.runId())) {
                continue;
            }
            Command.AssignRun assign = new Command.AssignRun(run.runId(), worker, leaseTicks);
            long index = raft.propose(CommandCodec.encode(assign));
            if (index > 0) {
                pending.put(index, new Pending(from, assign));
                assigningWorkers.add(worker);
                assigningRuns.add(run.runId());
            }
            return;
        }
    }

    private void onHeartbeat(NodeId from, WorkerMessages.Heartbeat hb) {
        RunView run = stateMachine.run(hb.runId()).orElse(null);
        boolean valid = run != null
                && (run.state() == RunState.ASSIGNED || run.state() == RunState.RUNNING)
                && run.token() == hb.token();
        if (valid) {
            leases.put(run.runId(), new Lease(run.token(), run.leaseTicks(), tick));
            if (run.state() == RunState.ASSIGNED) {
                // A heartbeat proves the worker started, even if its Started message was lost or misdirected.
                proposeOnce("T:" + run.runId() + ":" + run.token(), new Command.RunStarted(run.runId(), run.token()));
            }
        }
        transport.send(id, from, new WorkerMessages.HeartbeatAck(hb.runId(), hb.token(), valid));
    }

    private void onStarted(WorkerMessages.Started started) {
        proposeOnce("T:" + started.runId() + ":" + started.token(),
                new Command.RunStarted(started.runId(), started.token()));
    }

    private void onCompleted(NodeId from, WorkerMessages.Completed done) {
        Command.RunCompleted command = new Command.RunCompleted(done.runId(), done.token(), done.success(),
                done.detail());
        long index = proposeOnce("D:" + done.runId() + ":" + done.token(), command);
        if (index > 0) {
            pending.put(index, new Pending(from, command));
        }
    }

    // ---- membership (leader only) ----

    /** Tells the transport the address of every node in the current configuration, whenever it changes. */
    private void syncAddresses() {
        ClusterConfig current = raft.clusterConfig();
        if (current != syncedConfig) {
            syncedConfig = current;
            current.addresses().forEach(peerAddresses);
        }
    }

    private void onMembers(NodeId from, ClientMessages.Members m) {
        ClusterConfig cfg = raft.clusterConfig();
        String action = m.action();
        if (action.equals("LIST")) {
            replyMembers(from, m.requestId(), ClientMessages.Status.ACCEPTED, "ok");
            return;
        }
        NodeId target;
        try {
            target = m.nodeId().isEmpty() ? null : new NodeId(m.nodeId());
        } catch (IllegalArgumentException e) {
            replyMembers(from, m.requestId(), ClientMessages.Status.REJECTED, "bad node id");
            return;
        }
        if (target == null) {
            replyMembers(from, m.requestId(), ClientMessages.Status.REJECTED, "node id required");
            return;
        }
        long index;
        try {
            if (action.equals("ADD")) {
                if (cfg.isVoter(target) || cfg.isLearner(target)) {
                    replyMembers(from, m.requestId(), ClientMessages.Status.DUPLICATE, "already a member");
                    return;
                }
                if (m.address().isBlank()) {
                    replyMembers(from, m.requestId(), ClientMessages.Status.REJECTED, "address required");
                    return;
                }
                index = raft.addLearner(target, m.address());
            } else if (action.equals("PROMOTE")) {
                if (cfg.isVoter(target)) {
                    replyMembers(from, m.requestId(), ClientMessages.Status.DUPLICATE, "already a voter");
                    return;
                }
                if (!cfg.isLearner(target)) {
                    replyMembers(from, m.requestId(), ClientMessages.Status.REJECTED, "not a learner, add it first");
                    return;
                }
                index = raft.promote(target);
            } else if (action.equals("REMOVE")) {
                if (!cfg.isVoter(target) && !cfg.isLearner(target)) {
                    replyMembers(from, m.requestId(), ClientMessages.Status.DUPLICATE, "not a member");
                    return;
                }
                index = raft.remove(target);
            } else {
                replyMembers(from, m.requestId(), ClientMessages.Status.REJECTED, "unknown action");
                return;
            }
        } catch (IllegalArgumentException e) {
            replyMembers(from, m.requestId(), ClientMessages.Status.REJECTED, e.getMessage());
            return;
        }
        if (index <= 0) {
            replyMembers(from, m.requestId(), ClientMessages.Status.REJECTED,
                    "refused for now (a change is in progress, or the node is not caught up yet): retry");
            return;
        }
        memberPending.put(index, new ClientPending(from, m.requestId(), null));
    }

    /** Within one leader term nothing at an index of this leader's log is overwritten, so commit means done. */
    private void replyToCommittedMemberChanges() {
        memberPending.entrySet().removeIf(e -> {
            if (raft.commitIndex() < e.getKey()) {
                return false;
            }
            replyMembers(e.getValue().replyTo(), e.getValue().requestId(), ClientMessages.Status.ACCEPTED,
                    "committed");
            return true;
        });
    }

    private void replyMembers(NodeId to, long requestId, ClientMessages.Status status, String detail) {
        transport.send(id, to, new ClientMessages.MembersReply(requestId, status, detail, csv(raft.voters()),
                csv(raft.learners())));
    }

    private static String csv(Set<NodeId> nodes) {
        StringBuilder sb = new StringBuilder();
        for (NodeId n : nodes) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(n.value());
        }
        return sb.toString();
    }

    // ---- client requests (leader only) ----

    private void onClientSubmit(NodeId from, ClientMessages.Submit m) {
        Command.SubmitJob command = new Command.SubmitJob(m.jobId(), m.spec(), m.idempotencyKey(),
                clockMillis.getAsLong());
        long index = raft.propose(CommandCodec.encode(command));
        if (index > 0) {
            clientPending.put(index, new ClientPending(from, m.requestId(), command));
        } else {
            transport.send(id, from, new WorkerMessages.NotLeader(raft.leaderId()));
        }
    }

    private void onClientCancel(NodeId from, ClientMessages.Cancel m) {
        Command.CancelJob command = new Command.CancelJob(m.jobId());
        long index = raft.propose(CommandCodec.encode(command));
        if (index > 0) {
            clientPending.put(index, new ClientPending(from, m.requestId(), command));
        } else {
            transport.send(id, from, new WorkerMessages.NotLeader(raft.leaderId()));
        }
    }

    /** Reads go through ReadIndex: answered only after a majority confirmed this node is still leader. */
    private void onClientGetJob(NodeId from, ClientMessages.GetJob m) {
        startRead(from, m);
    }

    private void startRead(NodeId from, Message request) {
        long readId = nextReadId++;
        if (raft.beginRead(readId)) {
            readPending.put(readId, new PendingRead(from, request));
        } else {
            transport.send(id, from, new WorkerMessages.NotLeader(raft.leaderId()));
        }
    }

    private void serveReads() {
        for (long readId : raft.readyReads()) {
            PendingRead pr = readPending.remove(readId);
            if (pr == null) {
                continue;
            }
            if (pr.request() instanceof ClientMessages.GetJob m) {
                replyGetJob(pr.from(), m);
            } else if (pr.request() instanceof ClientMessages.ListJobs m) {
                transport.send(id, pr.from(), new ClientMessages.JobsReply(m.requestId(), stateMachine.jobs()));
            }
        }
    }

    private void replyGetJob(NodeId from, ClientMessages.GetJob m) {
        JobView job = stateMachine.job(m.jobId()).orElse(null);
        List<RunView> runs = new ArrayList<>();
        if (job != null) {
            for (RunView r : stateMachine.runs()) {
                if (r.jobId().equals(m.jobId())) {
                    runs.add(r);
                }
            }
        }
        transport.send(id, from, new ClientMessages.JobReply(m.requestId(), job != null, job, runs));
    }

    private void replyToClient(ClientPending cp, Command command, Outcome outcome) {
        ClientMessages.Status status = switch (outcome) {
            case APPLIED -> ClientMessages.Status.ACCEPTED;
            case DUPLICATE -> ClientMessages.Status.DUPLICATE;
            default -> ClientMessages.Status.REJECTED;
        };
        if (command instanceof Command.SubmitJob submit) {
            String jobId = submit.jobId();
            if (outcome == Outcome.DUPLICATE && submit.idempotencyKey() != null) {
                jobId = stateMachine.jobIdForKey(submit.idempotencyKey()).orElse(jobId);
            }
            transport.send(id, cp.replyTo(), new ClientMessages.SubmitReply(cp.requestId(), status, jobId,
                    outcome.name()));
        } else if (command instanceof Command.CancelJob) {
            transport.send(id, cp.replyTo(), new ClientMessages.CancelReply(cp.requestId(), status, outcome.name()));
        }
    }

    private void sendAssignment(NodeId to, RunView run) {
        JobSpec spec = stateMachine.job(run.jobId()).orElseThrow().spec();
        transport.send(id, to, new WorkerMessages.Assignment(run.workerId(), run.runId(), run.token(),
                spec.handler(), spec.args(), run.leaseTicks()));
    }

    /** Called on every node for every applied entry; only the leader that proposed it has a pending reply. */
    private void onApplied(long index, Command command, Outcome outcome) {
        ClientPending cp = clientPending.remove(index);
        if (cp != null && Objects.equals(cp.command(), command)) {
            replyToClient(cp, command, outcome);
        }
        Pending p = pending.remove(index);
        if (p == null) {
            return;
        }
        if (p.command() instanceof Command.AssignRun assign) {
            assigningWorkers.remove(assign.workerId());
            assigningRuns.remove(assign.runId());
        }
        if (!Objects.equals(p.command(), command)) {
            return; // the index now holds a different entry, so the reply no longer applies
        }
        if (command instanceof Command.AssignRun assign && outcome == Outcome.APPLIED) {
            sendAssignment(p.replyTo(), stateMachine.run(assign.runId()).orElseThrow());
        } else if (command instanceof Command.RunCompleted done) {
            boolean accepted = outcome == Outcome.APPLIED || outcome == Outcome.DUPLICATE;
            transport.send(id, p.replyTo(), new WorkerMessages.CompletedAck(done.runId(), done.token(), accepted));
        }
    }
}
