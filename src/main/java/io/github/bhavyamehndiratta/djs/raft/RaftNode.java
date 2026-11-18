package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.net.Transport;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * One Raft node. Single threaded and event driven: it has no threads and never
 * reads the clock. It changes only when {@link #tick()} or {@link #handle} is
 * called, which is what lets the simulator replay a whole cluster from a seed.
 *
 * <p>Covers leader election (PreVote, CheckQuorum), log replication, the commit
 * rule, snapshots, and membership changes.
 *
 * <p>Membership uses single-server changes: a configuration entry differs from
 * the previous one by at most one voter, and a node uses the latest
 * configuration in its log as soon as the entry is appended, committed or not.
 * A leader accepts a change only when no earlier change is still uncommitted and
 * it has already committed an entry of its own term. If an uncommitted
 * configuration entry is truncated, the node falls back to the previous one.
 */
public final class RaftNode {

    /** Most entries sent in one AppendEntries, so a far-behind follower catches up in chunks. */
    private static final int MAX_BATCH = 64;

    /** A learner may be promoted once it is at most this many entries behind. */
    private static final int CATCH_UP_ENTRIES = 50;

    private final NodeId id;
    private final Transport transport;
    private final Random random;
    private final RaftConfig config;
    private final PersistentState state;
    private final RaftLog log;
    private final StateMachine stateMachine;
    private final RaftObserver observer;
    private final SnapshotStore snapshots;
    private Snapshot latestSnapshot = null;
    private long tickCount = 0;
    private final Map<NodeId, Long> snapshotSentAt = new HashMap<>();
    private int snapshotsTaken = 0;
    private int snapshotsInstalled = 0;

    /** Effective configuration: the latest configuration entry in the log, or the base one. */
    private ClusterConfig cluster;
    /** Log index of the entry that set {@link #cluster}, or 0 if it comes from bootstrap or a snapshot. */
    private long clusterIndex = 0;
    /** Configuration as of the snapshot point (or bootstrap if there is no snapshot). */
    private ClusterConfig baseCluster;
    /** Configuration as of the last applied entry. This is what a snapshot records. */
    private ClusterConfig appliedCluster;
    private List<NodeId> voterPeers = List.of();
    private List<NodeId> replicaPeers = List.of();

    private Role role = Role.FOLLOWER;
    private NodeId leaderId = null;
    private int electionElapsed = 0;
    private int randomizedElectionTimeout;
    private int heartbeatElapsed = 0;
    private final Set<NodeId> votesGranted = new HashSet<>();
    private final Set<NodeId> recentlyActive = new HashSet<>();

    private long commitIndex = 0;
    private long lastApplied = 0;
    private final Map<NodeId, Long> nextIndex = new HashMap<>();
    private final Map<NodeId, Long> matchIndex = new HashMap<>();

    /** A ReadIndex read waiting for a majority to confirm this node is still leader. */
    private record PendingRead(long readId, long readIndex, long seq, Set<NodeId> acks) {
    }

    private long readSeqCounter = 0;
    private final Map<Long, PendingRead> reads = new LinkedHashMap<>();

    /** Convenience for tests that only care about elections: empty in-memory log, no state machine. */
    public RaftNode(NodeId id, Collection<NodeId> members, Transport transport, Random random,
                    RaftConfig config, PersistentState state, RaftObserver observer) {
        this(id, members, transport, random, config, state, new InMemoryRaftLog(), StateMachine.NONE, observer);
    }

    public RaftNode(NodeId id, Collection<NodeId> members, Transport transport, Random random,
                    RaftConfig config, PersistentState state, RaftLog log, StateMachine stateMachine,
                    RaftObserver observer) {
        this(id, members, transport, random, config, state, log, stateMachine, new InMemorySnapshotStore(),
                observer);
    }

    public RaftNode(NodeId id, Collection<NodeId> members, Transport transport, Random random,
                    RaftConfig config, PersistentState state, RaftLog log, StateMachine stateMachine,
                    SnapshotStore snapshots, RaftObserver observer) {
        this(id, bootstrap(id, members), transport, random, config, state, log, stateMachine, snapshots, observer);
    }

    /**
     * A node that is being added to a running cluster. It starts knowing no
     * configuration, so it never campaigns, and learns the real configuration
     * from the log or snapshot its leader sends.
     */
    public static RaftNode joining(NodeId id, Transport transport, Random random, RaftConfig config,
                                   PersistentState state, RaftLog log, StateMachine stateMachine,
                                   SnapshotStore snapshots, RaftObserver observer) {
        return new RaftNode(id, ClusterConfig.EMPTY, transport, random, config, state, log, stateMachine,
                snapshots, observer);
    }

    private static ClusterConfig bootstrap(NodeId id, Collection<NodeId> members) {
        if (!members.contains(id)) {
            throw new IllegalArgumentException("members must include this node");
        }
        return ClusterConfig.of(members);
    }

    private RaftNode(NodeId id, ClusterConfig bootstrap, Transport transport, Random random,
                     RaftConfig config, PersistentState state, RaftLog log, StateMachine stateMachine,
                     SnapshotStore snapshots, RaftObserver observer) {
        this.id = id;
        this.transport = transport;
        this.random = random;
        this.config = config;
        this.state = state;
        this.log = log;
        this.stateMachine = stateMachine;
        this.observer = observer;
        this.snapshots = snapshots;
        this.baseCluster = bootstrap;
        this.appliedCluster = bootstrap;
        setCluster(bootstrap, 0);
        resetElectionTimer();
        recoverSnapshot();
        recomputeCluster();
    }

    // ---- inputs ----

    /** Advance logical time by one tick. */
    public void tick() {
        tickCount++;
        if (role == Role.LEADER) {
            tickLeader();
        } else {
            electionElapsed++;
            if (electionElapsed >= randomizedElectionTimeout) {
                if (cluster.isVoter(id)) {
                    startPreVote();
                } else {
                    resetElectionTimer(); // learners and removed nodes never campaign
                }
            }
        }
    }

    /**
     * Client entry point. Appends a command if this node is leader and returns
     * its log index; returns -1 otherwise. The entry is only durable and
     * applied once it commits, which the caller learns from the state machine.
     */
    public long propose(String command) {
        Objects.requireNonNull(command, "null is reserved for no-op entries");
        if (ClusterConfig.isConfigCommand(command)) {
            throw new IllegalArgumentException("configuration entries are reserved, use addLearner/promote/remove");
        }
        if (role != Role.LEADER) {
            return -1;
        }
        log.append(new LogEntry(currentTerm(), command));
        advanceCommit();
        replicateAll();
        return log.lastIndex();
    }

    /**
     * ReadIndex, step 1 (leader only). Starts a linearizable read named {@code readId}: records the current
     * commit index and sends a heartbeat round tagged with a fresh sequence number. The read becomes ready
     * (see {@link #readyReads()}) once a majority of voters has answered a heartbeat sent after this call,
     * which proves this node was still leader after the read began, and everything up to the recorded commit
     * index has been applied. Returns false if this node is not leader, or has not yet committed an entry of
     * its own term (until then its commit index can lag the real one).
     */
    public boolean beginRead(long readId) {
        if (role != Role.LEADER || commitIndex == 0 || log.termAt(commitIndex) != currentTerm()) {
            return false;
        }
        PendingRead read = new PendingRead(readId, commitIndex, ++readSeqCounter, new HashSet<>());
        if (cluster.isVoter(id)) {
            read.acks().add(id);
        }
        reads.put(readId, read);
        replicateAll();
        return true;
    }

    /**
     * ReadIndex, step 2. Returns, and forgets, the reads that are now safe to answer from the applied state.
     * A node that stopped being leader drops its reads: the caller's client will retry elsewhere.
     */
    public List<Long> readyReads() {
        List<Long> ready = new ArrayList<>();
        if (role != Role.LEADER) {
            reads.clear();
            return ready;
        }
        var it = reads.values().iterator();
        while (it.hasNext()) {
            PendingRead r = it.next();
            int confirmed = 0;
            for (NodeId a : r.acks()) {
                if (cluster.isVoter(a)) {
                    confirmed++;
                }
            }
            if (confirmed >= quorum() && lastApplied >= r.readIndex()) {
                ready.add(r.readId());
                it.remove();
            }
        }
        return ready;
    }

    private void recordReadAck(NodeId from, long echoedSeq) {
        if (reads.isEmpty() || echoedSeq <= 0) {
            return;
        }
        for (PendingRead r : reads.values()) {
            if (r.seq() <= echoedSeq) {
                r.acks().add(from);
            }
        }
    }

    public void handle(NodeId from, Message message) {
        switch (message) {
            case RequestVote m -> onRequestVote(from, m);
            case RequestVoteResponse m -> onRequestVoteResponse(from, m);
            case AppendEntries m -> onAppendEntries(from, m);
            case AppendEntriesResponse m -> onAppendEntriesResponse(from, m);
            case InstallSnapshot m -> onInstallSnapshot(from, m);
            case InstallSnapshotResponse m -> onInstallSnapshotResponse(from, m);
            default -> throw new IllegalArgumentException("unknown message: " + message);
        }
    }

    // ---- membership changes (leader only) ----

    /** Starts replicating to {@code node} without letting it vote. Returns the entry's index, or -1 if refused. */
    public long addLearner(NodeId node) {
        return addLearner(node, null);
    }

    /** As {@link #addLearner(NodeId)}, also recording the node's network address in the configuration. */
    public long addLearner(NodeId node, String address) {
        if (!canChangeConfig() || cluster.isVoter(node) || cluster.isLearner(node)) {
            return -1;
        }
        return appendConfig(cluster.withLearner(node, address));
    }

    /** Makes a caught-up learner a voter. Returns the entry's index, or -1 if refused. */
    public long promote(NodeId node) {
        if (!canChangeConfig() || !cluster.isLearner(node)) {
            return -1;
        }
        Long match = matchIndex.get(node);
        // The learner must hold the entry that made it a learner (so it knows the configuration)
        // and be close to the end of the log.
        if (match == null || match < clusterIndex || match < log.lastIndex() - CATCH_UP_ENTRIES) {
            return -1;
        }
        return appendConfig(cluster.promoted(node));
    }

    /** Removes a voter or learner. Returns the entry's index, or -1 if refused. */
    public long remove(NodeId node) {
        if (!canChangeConfig() || !(cluster.isVoter(node) || cluster.isLearner(node))) {
            return -1;
        }
        if (cluster.isVoter(node) && cluster.voters().size() == 1) {
            return -1;
        }
        return appendConfig(cluster.without(node));
    }

    /**
     * One change at a time, and only once this leader has committed something
     * in its own term. Without the second rule a new leader could build on a
     * configuration that another leader's log is about to overwrite.
     */
    private boolean canChangeConfig() {
        return role == Role.LEADER
                && clusterIndex <= commitIndex
                && commitIndex > 0
                && log.termAt(commitIndex) == currentTerm();
    }

    private long appendConfig(ClusterConfig next) {
        log.append(new LogEntry(currentTerm(), next.encode()));
        setCluster(next, log.lastIndex());
        advanceCommit();
        replicateAll();
        return log.lastIndex();
    }

    // ---- configuration bookkeeping ----

    private void setCluster(ClusterConfig next, long index) {
        cluster = next;
        clusterIndex = index;
        voterPeers = peersOf(next.voters());
        replicaPeers = peersOf(next.all());
        if (role == Role.LEADER) {
            syncReplicaState();
        }
    }

    private List<NodeId> peersOf(Set<NodeId> nodes) {
        Set<NodeId> others = new TreeSet<>(nodes);
        others.remove(id);
        return List.copyOf(others);
    }

    /** Leader: keep one nextIndex/matchIndex pair per node it replicates to, and no others. */
    private void syncReplicaState() {
        Set<NodeId> want = new HashSet<>(replicaPeers);
        nextIndex.keySet().retainAll(want);
        matchIndex.keySet().retainAll(want);
        snapshotSentAt.keySet().retainAll(want);
        for (NodeId peer : replicaPeers) {
            nextIndex.putIfAbsent(peer, log.lastIndex() + 1);
            matchIndex.putIfAbsent(peer, 0L);
        }
    }

    /** Finds the latest configuration entry in the log. Needed after a configuration entry is truncated. */
    private void recomputeCluster() {
        for (long i = log.lastIndex(); i > log.snapshotIndex(); i--) {
            LogEntry e = log.get(i);
            if (isConfigEntry(e)) {
                setCluster(ClusterConfig.decode(e.command()), i);
                return;
            }
        }
        setCluster(baseCluster, 0);
    }

    private static boolean isConfigEntry(LogEntry e) {
        return !e.isNoop() && ClusterConfig.isConfigCommand(e.command());
    }

    // ---- leader ----

    private void tickLeader() {
        heartbeatElapsed++;
        electionElapsed++;
        if (electionElapsed >= config.electionTimeoutTicks()) {
            electionElapsed = 0;
            // CheckQuorum: a leader that cannot reach a majority of voters steps down.
            int active = cluster.isVoter(id) ? 1 : 0;
            for (NodeId v : voterPeers) {
                if (recentlyActive.contains(v)) {
                    active++;
                }
            }
            recentlyActive.clear();
            if (active < quorum()) {
                becomeFollower(currentTerm(), null);
                return;
            }
        }
        if (heartbeatElapsed >= config.heartbeatIntervalTicks()) {
            heartbeatElapsed = 0;
            replicateAll();
        }
    }

    private void becomeLeader() {
        setRole(Role.LEADER);
        leaderId = id;
        reads.clear();
        heartbeatElapsed = 0;
        electionElapsed = 0;
        recentlyActive.clear();
        snapshotSentAt.clear();
        nextIndex.clear();
        matchIndex.clear();
        syncReplicaState();
        // A no-op in the new term lets the leader commit everything before it,
        // because entries from older terms cannot be committed by counting.
        log.append(LogEntry.noop(currentTerm()));
        advanceCommit();
        replicateAll();
    }

    private void replicateAll() {
        if (role != Role.LEADER) {
            return;
        }
        for (NodeId peer : replicaPeers) {
            replicateTo(peer);
        }
    }

    private void replicateTo(NodeId peer) {
        if (role != Role.LEADER) {
            return;
        }
        long next = nextIndex.get(peer);
        if (next <= log.snapshotIndex()) {
            sendSnapshot(peer); // the entries it needs were compacted away
            return;
        }
        long prev = next - 1;
        List<LogEntry> batch = new ArrayList<>();
        for (long i = next; i <= log.lastIndex() && batch.size() < MAX_BATCH; i++) {
            batch.add(log.get(i));
        }
        transport.send(id, peer,
                new AppendEntries(currentTerm(), id, prev, log.termAt(prev), batch, commitIndex, readSeqCounter));
    }

    /**
     * Moves the commit index forward. An entry commits once a majority of the
     * voters holds it AND it is from the leader's own term. Older-term entries
     * then commit indirectly: committing a current-term entry commits
     * everything before it. Learners never count.
     */
    private void advanceCommit() {
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n) != currentTerm()) {
                break;
            }
            int count = cluster.isVoter(id) ? 1 : 0;
            for (NodeId peer : voterPeers) {
                if (matchIndex.get(peer) >= n) {
                    count++;
                }
            }
            if (count >= quorum()) {
                commitIndex = n;
                applyCommitted();
                break;
            }
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            stateMachine.apply(lastApplied, entry);
            if (isConfigEntry(entry)) {
                appliedCluster = ClusterConfig.decode(entry.command());
            }
        }
        maybeSnapshot();
        // A leader that removed itself steps down once that change has committed.
        if (role == Role.LEADER && !cluster.isVoter(id) && clusterIndex <= commitIndex) {
            becomeFollower(currentTerm(), null);
        }
    }

    // ---- elections ----

    private void startPreVote() {
        setRole(Role.PRE_CANDIDATE);
        leaderId = null;
        resetElectionTimer();
        votesGranted.clear();
        votesGranted.add(id);
        if (votes() >= quorum()) {
            startElection();
            return;
        }
        long proposedTerm = currentTerm() + 1;
        for (NodeId peer : voterPeers) {
            transport.send(id, peer, new RequestVote(proposedTerm, id, lastLogIndex(), lastLogTerm(), true));
        }
    }

    private void startElection() {
        state.setTermAndVote(currentTerm() + 1, id);
        setRole(Role.CANDIDATE);
        leaderId = null;
        resetElectionTimer();
        votesGranted.clear();
        votesGranted.add(id);
        if (votes() >= quorum()) {
            becomeLeader();
            return;
        }
        for (NodeId peer : voterPeers) {
            transport.send(id, peer, new RequestVote(currentTerm(), id, lastLogIndex(), lastLogTerm(), false));
        }
    }

    private void onRequestVote(NodeId from, RequestVote m) {
        // Votes depend on the log and the lease only, never on whether this node's own, possibly
        // stale, configuration lists the candidate: refusing an unknown candidate can deadlock
        // elections. A removed node cannot win anyway. It never receives its removal entry, so a
        // majority of voters reject it as behind, and its PreVote fails for the same reason.
        // Heard from a live leader recently: refuse to help unseat it.
        boolean inLease = leaderId != null && electionElapsed < config.electionTimeoutTicks();
        boolean logOk = candidateLogUpToDate(m.lastLogIndex(), m.lastLogTerm());

        if (m.preVote()) {
            // PreVote never changes this node's term or vote.
            boolean grant = !inLease && m.term() > currentTerm() && logOk;
            transport.send(id, from, new RequestVoteResponse(grant ? m.term() : currentTerm(), grant, true));
            return;
        }
        if (inLease && m.term() > currentTerm()) {
            return;
        }
        if (m.term() > currentTerm()) {
            becomeFollower(m.term(), null);
        }
        NodeId votedFor = state.votedFor();
        boolean grant = m.term() == currentTerm()
                && (votedFor == null || votedFor.equals(m.candidate()))
                && logOk;
        if (grant) {
            state.setTermAndVote(currentTerm(), m.candidate()); // durable before we reply
            electionElapsed = 0;
        }
        transport.send(id, from, new RequestVoteResponse(currentTerm(), grant, false));
    }

    private void onRequestVoteResponse(NodeId from, RequestVoteResponse m) {
        if (m.preVote()) {
            if (role != Role.PRE_CANDIDATE) {
                return;
            }
            if (!m.granted() && m.term() > currentTerm()) {
                becomeFollower(m.term(), null);
                return;
            }
            if (m.granted() && m.term() == currentTerm() + 1) {
                votesGranted.add(from);
                if (votes() >= quorum()) {
                    startElection();
                }
            }
            return;
        }
        if (m.term() > currentTerm()) {
            becomeFollower(m.term(), null);
            return;
        }
        if (role != Role.CANDIDATE || m.term() != currentTerm() || !m.granted()) {
            return;
        }
        votesGranted.add(from);
        if (votes() >= quorum()) {
            becomeLeader();
        }
    }

    // ---- replication ----

    private void onAppendEntries(NodeId from, AppendEntries m) {
        if (m.term() < currentTerm()) {
            // Tell a stale leader about the newer term so it steps down.
            transport.send(id, from, new AppendEntriesResponse(currentTerm(), false));
            return;
        }
        if (role == Role.LEADER && m.term() == currentTerm()) {
            throw new IllegalStateException("two leaders in term " + m.term() + ": " + id + " and " + from);
        }
        if (m.term() > currentTerm() || role != Role.FOLLOWER || !from.equals(leaderId)) {
            becomeFollower(m.term(), from);
        }
        electionElapsed = 0;

        long prev = m.prevLogIndex();
        long prevTerm = m.prevLogTerm();
        List<LogEntry> incoming = m.entries();
        if (prev < log.snapshotIndex()) {
            // The leader is behind our snapshot. Everything up to it is committed and identical, so skip it.
            long skip = log.snapshotIndex() - prev;
            if (skip >= incoming.size()) {
                transport.send(id, from, new AppendEntriesResponse(currentTerm(), true, log.snapshotIndex(), 0, 0, m.readSeq()));
                return;
            }
            incoming = incoming.subList((int) skip, incoming.size());
            prev = log.snapshotIndex();
            prevTerm = log.snapshotTerm();
        }
        if (prev > log.lastIndex()) {
            // Log too short: ask the leader to resume right after our last entry.
            transport.send(id, from, new AppendEntriesResponse(currentTerm(), false, 0, 0, log.lastIndex() + 1, m.readSeq()));
            return;
        }
        if (log.termAt(prev) != prevTerm) {
            // Log matching failed: report the conflicting term and where it starts.
            long conflictTerm = log.termAt(prev);
            long conflictIndex = prev;
            while (conflictIndex > log.snapshotIndex() + 1 && log.termAt(conflictIndex - 1) == conflictTerm) {
                conflictIndex--;
            }
            transport.send(id, from, new AppendEntriesResponse(currentTerm(), false, 0, conflictTerm, conflictIndex, m.readSeq()));
            return;
        }

        long index = prev;
        boolean recompute = false;
        for (LogEntry entry : incoming) {
            index++;
            if (index <= log.lastIndex()) {
                if (log.termAt(index) == entry.term()) {
                    continue; // already have it: a duplicate or reordered message must change nothing
                }
                if (index <= commitIndex) {
                    throw new IllegalStateException("leader tried to overwrite committed entry " + index);
                }
                if (index <= clusterIndex) {
                    recompute = true; // the configuration entry in force is being thrown away
                }
                log.truncateFrom(index); // conflicting suffix from an old leader
            }
            log.append(entry);
            if (!recompute && isConfigEntry(entry)) {
                setCluster(ClusterConfig.decode(entry.command()), index);
            }
        }
        if (recompute) {
            recomputeCluster();
        }

        long lastNew = prev + incoming.size();
        long newCommit = Math.min(m.leaderCommit(), lastNew);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }
        transport.send(id, from, new AppendEntriesResponse(currentTerm(), true, lastNew, 0, 0, m.readSeq()));
    }

    private void onAppendEntriesResponse(NodeId from, AppendEntriesResponse m) {
        if (m.term() > currentTerm()) {
            becomeFollower(m.term(), null);
            return;
        }
        if (role != Role.LEADER || m.term() != currentTerm() || !matchIndex.containsKey(from)) {
            return;
        }
        recentlyActive.add(from);
        recordReadAck(from, m.readSeq());

        if (m.success()) {
            if (m.matchIndex() > matchIndex.get(from)) {
                matchIndex.put(from, m.matchIndex());
                nextIndex.put(from, m.matchIndex() + 1);
                advanceCommit();
            }
            if (role == Role.LEADER && nextIndex.containsKey(from) && nextIndex.get(from) <= log.lastIndex()) {
                replicateTo(from); // still behind: keep streaming
            }
            return;
        }

        long hint;
        if (m.conflictTerm() == 0) {
            hint = m.conflictIndex();
        } else {
            long last = lastIndexWithTerm(m.conflictTerm());
            hint = last > 0 ? last + 1 : m.conflictIndex();
        }
        long next = Math.max(matchIndex.get(from) + 1, Math.min(hint, nextIndex.get(from) - 1));
        nextIndex.put(from, next);
        replicateTo(from);
    }

    private long lastIndexWithTerm(long term) {
        for (long i = log.lastIndex(); i > log.snapshotIndex(); i--) {
            if (log.termAt(i) == term) {
                return i;
            }
            if (log.termAt(i) < term) {
                return 0;
            }
        }
        return 0;
    }

    // ---- snapshots ----

    /** A snapshot's data is the configuration as of that index, a newline, then the state machine's own data. */
    static String withConfig(ClusterConfig cluster, String data) {
        return cluster.encode() + "\n" + data;
    }

    private record Unwrapped(ClusterConfig cluster, String data) {
    }

    private static Unwrapped unwrap(String wrapped) {
        int nl = wrapped.indexOf('\n');
        if (nl < 0 || !ClusterConfig.isConfigCommand(wrapped)) {
            throw new IllegalStateException("snapshot has no configuration header, refusing to start");
        }
        return new Unwrapped(ClusterConfig.decode(wrapped.substring(0, nl)), wrapped.substring(nl + 1));
    }

    /** On start: load the latest snapshot, bring the log in line with it, and restore the state machine. */
    private void recoverSnapshot() {
        Snapshot s = snapshots.load();
        if (s == null) {
            if (log.snapshotIndex() > 0) {
                throw new IllegalStateException("log is compacted to " + log.snapshotIndex()
                        + " but there is no snapshot, refusing to start");
            }
            return;
        }
        if (log.snapshotIndex() > s.index()) {
            throw new IllegalStateException("log is compacted past the snapshot, refusing to start");
        }
        Unwrapped u = unwrap(s.data());
        if (log.snapshotIndex() < s.index()) {
            // Crashed after saving the snapshot but before the log shrank: finish the job.
            if (log.lastIndex() >= s.index() && log.termAt(s.index()) == s.term()) {
                log.compactTo(s.index(), s.term());
            } else {
                log.reset(s.index(), s.term());
            }
        }
        stateMachine.restore(u.data());
        baseCluster = u.cluster();
        appliedCluster = u.cluster();
        latestSnapshot = s;
        commitIndex = s.index();
        lastApplied = s.index();
    }

    /** Takes a snapshot once enough entries have been applied since the last one. */
    private void maybeSnapshot() {
        int threshold = config.snapshotThresholdEntries();
        long base = latestSnapshot == null ? 0 : latestSnapshot.index();
        if (threshold <= 0 || !stateMachine.supportsSnapshots() || lastApplied - base < threshold) {
            return;
        }
        long term = log.termAt(lastApplied);
        Snapshot s = new Snapshot(lastApplied, term, withConfig(appliedCluster, stateMachine.snapshot()));
        snapshots.save(s); // durable before the log shrinks
        latestSnapshot = s;
        log.compactTo(lastApplied, term);
        baseCluster = appliedCluster;
        snapshotsTaken++;
    }

    private void sendSnapshot(NodeId peer) {
        Long sent = snapshotSentAt.get(peer);
        if (sent != null && tickCount - sent < Math.max(2, config.electionTimeoutTicks() / 2)) {
            return; // one is already on its way
        }
        snapshotSentAt.put(peer, tickCount);
        transport.send(id, peer, new InstallSnapshot(currentTerm(), id, latestSnapshot.index(),
                latestSnapshot.term(), latestSnapshot.data()));
    }

    private void onInstallSnapshot(NodeId from, InstallSnapshot m) {
        if (m.term() < currentTerm()) {
            transport.send(id, from, new InstallSnapshotResponse(currentTerm(), 0));
            return;
        }
        if (role == Role.LEADER && m.term() == currentTerm()) {
            throw new IllegalStateException("two leaders in term " + m.term() + ": " + id + " and " + from);
        }
        if (m.term() > currentTerm() || role != Role.FOLLOWER || !from.equals(leaderId)) {
            becomeFollower(m.term(), from);
        }
        electionElapsed = 0;

        long index = m.lastIncludedIndex();
        if (index <= commitIndex) {
            // We already have everything this snapshot covers. Tell the leader how far we are.
            transport.send(id, from, new InstallSnapshotResponse(currentTerm(), commitIndex));
            return;
        }
        Unwrapped u = unwrap(m.data());
        Snapshot s = new Snapshot(index, m.lastIncludedTerm(), m.data());
        snapshots.save(s); // durable before the log changes
        if (index <= log.lastIndex() && log.termAt(index) == s.term()) {
            log.compactTo(index, s.term()); // our log agrees with the snapshot: keep what follows it
        } else {
            log.reset(index, s.term());
        }
        stateMachine.restore(u.data());
        latestSnapshot = s;
        baseCluster = u.cluster();
        appliedCluster = u.cluster();
        commitIndex = index;
        lastApplied = index;
        recomputeCluster();
        snapshotsInstalled++;
        transport.send(id, from, new InstallSnapshotResponse(currentTerm(), index));
    }

    private void onInstallSnapshotResponse(NodeId from, InstallSnapshotResponse m) {
        if (m.term() > currentTerm()) {
            becomeFollower(m.term(), null);
            return;
        }
        if (role != Role.LEADER || m.term() != currentTerm() || !matchIndex.containsKey(from)) {
            return;
        }
        recentlyActive.add(from);
        snapshotSentAt.remove(from);
        if (m.matchIndex() > matchIndex.get(from)) {
            matchIndex.put(from, m.matchIndex());
            advanceCommit();
        }
        if (role == Role.LEADER && matchIndex.containsKey(from)) {
            nextIndex.put(from, matchIndex.get(from) + 1);
            if (nextIndex.get(from) <= log.lastIndex()) {
                replicateTo(from);
            }
        }
    }

    // ---- helpers ----

    private void becomeFollower(long term, NodeId leader) {
        if (term > currentTerm()) {
            state.setTermAndVote(term, null);
        }
        setRole(Role.FOLLOWER);
        leaderId = leader;
        reads.clear();
        resetElectionTimer();
    }

    private void setRole(Role newRole) {
        role = newRole;
        observer.onRoleChange(id, newRole, currentTerm());
    }

    private void resetElectionTimer() {
        electionElapsed = 0;
        int base = config.electionTimeoutTicks();
        randomizedElectionTimeout = base + random.nextInt(base);
    }

    private int quorum() {
        return cluster.voters().size() / 2 + 1;
    }

    /** Votes held, counting only nodes that are voters in the current configuration. */
    private int votes() {
        int n = 0;
        for (NodeId v : votesGranted) {
            if (cluster.isVoter(v)) {
                n++;
            }
        }
        return n;
    }

    private boolean candidateLogUpToDate(long candidateLastIndex, long candidateLastTerm) {
        return candidateLastTerm > lastLogTerm()
                || (candidateLastTerm == lastLogTerm() && candidateLastIndex >= lastLogIndex());
    }

    private long lastLogTerm() {
        return log.lastTerm();
    }

    // ---- read-only views for tests and tooling ----

    public NodeId id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public long currentTerm() {
        return state.currentTerm();
    }

    public NodeId leaderId() {
        return leaderId;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public long lastLogIndex() {
        return log.lastIndex();
    }

    public RaftLog log() {
        return log;
    }

    public long snapshotIndex() {
        return log.snapshotIndex();
    }

    public int snapshotsTaken() {
        return snapshotsTaken;
    }

    public int snapshotsInstalled() {
        return snapshotsInstalled;
    }

    public ClusterConfig clusterConfig() {
        return cluster;
    }

    public Set<NodeId> voters() {
        return cluster.voters();
    }

    public Set<NodeId> learners() {
        return cluster.learners();
    }

    public boolean isVoter(NodeId node) {
        return cluster.isVoter(node);
    }
}
