package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Seeded chaos with membership changes on top of the usual faults. Nodes are
 * added as learners, promoted, and removed (including the leader itself and
 * nodes that are crashed or partitioned at the time), while proposals, crashes,
 * restarts, partitions and a flaky network continue. One seed fixes everything.
 *
 * <p>On top of the Raft safety checks (one leader per term, state machine
 * safety, log matching, leader completeness) it checks the single-server rule
 * on the committed history: each committed configuration differs from the one
 * before it by at most one node, and by at most one voter. After healing, the
 * nodes that are still members must converge, keep every acknowledged entry,
 * and commit a new command. Removed nodes are excluded from convergence.
 */
final class MembershipChaosHarness {

    record Result(long seed, List<String> violations, int proposals, int committedCommands, int crashes,
                  int partitions, int learnersAdded, int promotions, int removals, int committedConfigs,
                  int snapshotsTaken, int snapshotInstalls) {
        boolean ok() {
            return violations.isEmpty();
        }
    }

    private static final int MAX_NODES = 8;

    private MembershipChaosHarness() {
    }

    static Result run(long seed) {
        Random rnd = new Random(seed * 2_654_435_761L + 41);
        int size = rnd.nextBoolean() ? 3 : 5;
        Random snapshotRandom = new Random(seed ^ 0x2545F491L);
        int threshold = snapshotRandom.nextInt(3) == 0 ? 0 : 3 + snapshotRandom.nextInt(10);
        TestCluster c = new TestCluster(size, seed, threshold);
        ClusterConfig initial = ClusterConfig.of(c.members());
        List<String> violations = new ArrayList<>();
        int proposals = 0;
        int crashes = 0;
        int partitions = 0;
        int learnersAdded = 0;
        int promotions = 0;
        int removals = 0;
        try {
            int rounds = 400 + rnd.nextInt(500);
            for (int round = 0; round < rounds && violations.isEmpty(); round++) {
                int action = rnd.nextInt(100);
                if (action < 34) {
                    List<RaftNode> leaders = c.leaders();
                    if (!leaders.isEmpty()) {
                        String command = "cmd-" + seed + "-" + proposals;
                        if (leaders.get(rnd.nextInt(leaders.size())).propose(command) > 0) {
                            proposals++;
                        }
                    }
                } else if (action < 41) {
                    List<NodeId> up = c.upNodes();
                    if (!up.isEmpty()) {
                        c.crash(up.get(rnd.nextInt(up.size())));
                        crashes++;
                    }
                } else if (action < 43) {
                    List<RaftNode> leaders = c.leaders();
                    if (!leaders.isEmpty()) {
                        c.crash(leaders.get(rnd.nextInt(leaders.size())).id());
                        crashes++;
                    }
                } else if (action < 54) {
                    List<NodeId> down = new ArrayList<>();
                    for (NodeId id : c.members()) {
                        if (!c.isUp(id)) {
                            down.add(id);
                        }
                    }
                    if (!down.isEmpty()) {
                        c.restart(down.get(rnd.nextInt(down.size())));
                    }
                } else if (action < 60) {
                    List<NodeId> shuffled = new ArrayList<>(c.members());
                    Collections.shuffle(shuffled, rnd);
                    int cut = 1 + rnd.nextInt(shuffled.size() - 1);
                    c.net.partition(Set.copyOf(shuffled.subList(0, cut)),
                            Set.copyOf(shuffled.subList(cut, shuffled.size())));
                    partitions++;
                } else if (action < 66) {
                    c.net.heal();
                } else if (action < 70) {
                    double[] drops = {0, 0.05, 0.15};
                    double[] dups = {0, 0.05, 0.2};
                    long[] maxDelays = {1, 3, 10};
                    c.net.setDropRate(drops[rnd.nextInt(drops.length)]);
                    c.net.setDuplicateRate(dups[rnd.nextInt(dups.length)]);
                    c.net.setDelayRange(1, maxDelays[rnd.nextInt(maxDelays.length)]);
                } else if (action < 76) {
                    if (c.members().size() < MAX_NODES) {
                        c.addNode(new NodeId("n" + (c.members().size() + 1)));
                    }
                } else if (action < 90) {
                    List<RaftNode> leaders = c.leaders();
                    if (!leaders.isEmpty()) {
                        RaftNode leader = leaders.get(rnd.nextInt(leaders.size()));
                        int op = rnd.nextInt(3);
                        if (op == 0) {
                            List<NodeId> candidates = new ArrayList<>(c.members());
                            candidates.removeAll(leader.clusterConfig().all());
                            if (!candidates.isEmpty()
                                    && leader.addLearner(candidates.get(rnd.nextInt(candidates.size()))) > 0) {
                                learnersAdded++;
                            }
                        } else if (op == 1) {
                            List<NodeId> learners = new ArrayList<>(leader.learners());
                            if (!learners.isEmpty() && leader.promote(learners.get(rnd.nextInt(learners.size()))) > 0) {
                                promotions++;
                            }
                        } else {
                            List<NodeId> current = new ArrayList<>(leader.clusterConfig().all());
                            NodeId target = current.get(rnd.nextInt(current.size()));
                            boolean tooFew = leader.isVoter(target) && leader.voters().size() <= 2;
                            if (!tooFew && leader.remove(target) > 0) {
                                removals++;
                            }
                        }
                    }
                }
                c.runFor(1 + rnd.nextInt(8));
                violations.addAll(ChaosHarness.checkContinuously(c, round));
                if (round % 10 == 0) {
                    violations.addAll(configChainViolations(c.appliedEntries(), initial));
                }
            }
            Set<NodeId> finalMembers = new TreeSet<>();
            if (violations.isEmpty()) {
                finalMembers = settle(c, violations);
            }
            if (violations.isEmpty()) {
                violations.addAll(checkFinal(c, finalMembers, initial));
            }
        } catch (RuntimeException e) {
            violations.add("exception: " + e);
        }
        int committed = 0;
        int configs = 0;
        for (LogEntry entry : c.appliedEntries().values()) {
            if (entry.isNoop()) {
                continue;
            }
            if (ClusterConfig.isConfigCommand(entry.command())) {
                configs++;
            } else {
                committed++;
            }
        }
        return new Result(seed, List.copyOf(violations), proposals, committed, crashes, partitions,
                learnersAdded, promotions, removals, configs, c.snapshotsTaken(), c.snapshotInstalls());
    }

    /** Heals every fault, restarts every node, and proves the remaining cluster makes progress. */
    private static Set<NodeId> settle(TestCluster c, List<String> violations) {
        c.net.heal();
        c.net.setDropRate(0);
        c.net.setDuplicateRate(0);
        c.net.setDelayRange(1, 1);
        for (NodeId id : c.members()) {
            if (!c.isUp(id)) {
                c.restart(id);
            }
        }
        c.runFor(400);
        RaftNode leader;
        try {
            leader = c.awaitSingleLeader(800);
        } catch (AssertionError e) {
            StringBuilder sb = new StringBuilder("liveness: no single leader 800 ticks after healing; nodes:");
            for (NodeId id : c.members()) {
                RaftNode n = c.node(id);
                sb.append(" [").append(id.value()).append(c.isUp(id) ? " up " : " down ").append(n.role())
                        .append(" term=").append(n.currentTerm()).append(" voters=").append(ids(n.voters()))
                        .append(" learners=").append(ids(n.learners())).append(" commit=").append(n.commitIndex())
                        .append(" last=").append(n.lastLogIndex()).append(']');
            }
            violations.add(sb.toString());
            return Set.of();
        }
        long index = c.propose("final-marker");
        if (index < 0) {
            violations.add("liveness: no leader accepted the final command");
            return Set.of();
        }
        Set<NodeId> members = new TreeSet<>(leader.clusterConfig().all());
        for (int i = 0; i < 800 && !committedOn(c, members, index); i++) {
            c.tick();
        }
        if (!committedOn(c, members, index)) {
            violations.add("liveness: final command not committed on every remaining member " + members);
        }
        return members;
    }

    private static String ids(Set<NodeId> nodes) {
        StringBuilder sb = new StringBuilder();
        for (NodeId n : nodes) {
            sb.append(n.value()).append(',');
        }
        return sb.toString();
    }

    private static boolean committedOn(TestCluster c, Set<NodeId> members, long index) {
        for (NodeId id : members) {
            if (c.node(id).commitIndex() < index) {
                return false;
            }
        }
        return true;
    }

    /** Single-server rule on the committed history: each configuration changes at most one node, and one voter. */
    static List<String> configChainViolations(Map<Long, LogEntry> applied, ClusterConfig initial) {
        List<String> bad = new ArrayList<>();
        ClusterConfig prev = initial;
        for (Map.Entry<Long, LogEntry> e : applied.entrySet()) {
            LogEntry entry = e.getValue();
            if (entry.isNoop() || !ClusterConfig.isConfigCommand(entry.command())) {
                continue;
            }
            ClusterConfig cur = ClusterConfig.decode(entry.command());
            if (symmetricDifference(prev.voters(), cur.voters()) > 1) {
                bad.add("config at index " + e.getKey() + " changes more than one voter: " + prev + " -> " + cur);
            }
            if (symmetricDifference(prev.all(), cur.all()) > 1) {
                bad.add("config at index " + e.getKey() + " changes more than one node: " + prev + " -> " + cur);
            }
            prev = cur;
        }
        return bad;
    }

    private static int symmetricDifference(Set<NodeId> a, Set<NodeId> b) {
        Set<NodeId> only = new TreeSet<>(a);
        only.removeAll(b);
        Set<NodeId> other = new TreeSet<>(b);
        other.removeAll(a);
        return only.size() + other.size();
    }

    /** Run once healed. Only the nodes still in the configuration must have converged. */
    static List<String> checkFinal(TestCluster c, Set<NodeId> members, ClusterConfig initial) {
        List<String> bad = new ArrayList<>();
        if (!c.electionSafetyViolations().isEmpty()) {
            bad.add("two leaders in one term: " + c.electionSafetyViolations());
        }
        bad.addAll(c.stateMachineViolations());
        bad.addAll(c.logMatchingViolations());

        Map<Long, LogEntry> applied = c.appliedEntries();
        long expected = 1;
        Set<String> seenCommands = new HashSet<>();
        for (Map.Entry<Long, LogEntry> e : applied.entrySet()) {
            if (e.getKey() != expected) {
                bad.add("applied entries have a gap before index " + e.getKey());
                break;
            }
            expected++;
            LogEntry entry = e.getValue();
            if (!entry.isNoop() && !ClusterConfig.isConfigCommand(entry.command())
                    && !seenCommands.add(entry.command())) {
                bad.add("command applied twice: " + entry.command());
            }
        }
        bad.addAll(configChainViolations(applied, initial));

        Set<Long> commitIndexes = new TreeSet<>();
        for (NodeId id : members) {
            RaftLog log = c.log(id);
            for (Map.Entry<Long, LogEntry> e : applied.entrySet()) {
                long index = e.getKey();
                if (index <= log.snapshotIndex()) {
                    continue;
                }
                if (index > log.lastIndex() || !log.get(index).equals(e.getValue())) {
                    bad.add("lost acknowledged entry " + index + " on " + id);
                    break;
                }
            }
            commitIndexes.add(c.node(id).commitIndex());
            String want = SnapshotRecorder.expected(applied, c.node(id).lastApplied());
            if (!c.stateOf(id).equals(want)) {
                bad.add("state machine on " + id + " diverged: " + c.stateOf(id) + " but expected " + want);
            }
        }
        if (commitIndexes.size() > 1) {
            bad.add("members disagree on commit index after healing: " + commitIndexes);
        }
        return bad;
    }
}
