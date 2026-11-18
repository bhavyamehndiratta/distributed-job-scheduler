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
 * Seeded failure injection. One seed fixes everything: cluster size, the
 * random schedule of proposals, crashes, restarts, partitions and network
 * flakiness, and the node timers. Re-running a seed replays the same run, so a
 * failure is a reproducible bug report. After the chaos phase the harness heals
 * everything and checks that the cluster still works and that nothing
 * acknowledged was lost.
 *
 * <p>An entry is acknowledged once any node has applied it (it is committed).
 * Checks: at most one leader per term, no two nodes apply different entries at
 * one index, log matching, leader completeness (a leader holds every entry
 * committed in its term or earlier), no lost acknowledged entry, committed
 * logs agree on every node, no command applied twice, and liveness after the
 * network heals.
 */
final class ChaosHarness {

    record Result(long seed, int clusterSize, List<String> violations, int proposals,
                  int committedCommands, int crashes, int partitions, int electedTerms,
                  int snapshotsTaken, int snapshotInstalls) {
        boolean ok() {
            return violations.isEmpty();
        }
    }

    private ChaosHarness() {
    }

    static Result run(long seed) {
        Random rnd = new Random(seed * 2_654_435_761L + 17);
        int size = rnd.nextBoolean() ? 3 : 5;
        Random snapshotRandom = new Random(seed ^ 0x5bd1e995L);
        int threshold = snapshotRandom.nextInt(3) == 0 ? 0 : 3 + snapshotRandom.nextInt(10);
        TestCluster c = new TestCluster(size, seed, threshold);
        List<String> violations = new ArrayList<>();
        int proposals = 0;
        int crashes = 0;
        int partitions = 0;
        try {
            int rounds = 300 + rnd.nextInt(500);
            for (int round = 0; round < rounds && violations.isEmpty(); round++) {
                int action = rnd.nextInt(100);
                if (action < 40) {
                    List<RaftNode> leaders = c.leaders();
                    if (!leaders.isEmpty()) {
                        String command = "cmd-" + seed + "-" + proposals;
                        if (leaders.get(rnd.nextInt(leaders.size())).propose(command) > 0) {
                            proposals++;
                        }
                    }
                } else if (action < 48) {
                    List<NodeId> up = c.upNodes();
                    if (!up.isEmpty()) {
                        c.crash(up.get(rnd.nextInt(up.size())));
                        crashes++;
                    }
                } else if (action < 50) {
                    List<RaftNode> leaders = c.leaders();
                    if (!leaders.isEmpty()) {
                        c.crash(leaders.get(rnd.nextInt(leaders.size())).id());
                        crashes++;
                    }
                } else if (action < 62) {
                    List<NodeId> down = new ArrayList<>();
                    for (NodeId id : c.members()) {
                        if (!c.isUp(id)) {
                            down.add(id);
                        }
                    }
                    if (!down.isEmpty()) {
                        c.restart(down.get(rnd.nextInt(down.size())));
                    }
                } else if (action < 70) {
                    List<NodeId> shuffled = new ArrayList<>(c.members());
                    Collections.shuffle(shuffled, rnd);
                    int cut = 1 + rnd.nextInt(size - 1);
                    c.net.partition(Set.copyOf(shuffled.subList(0, cut)),
                            Set.copyOf(shuffled.subList(cut, size)));
                    partitions++;
                } else if (action < 78) {
                    c.net.heal();
                } else if (action < 84) {
                    double[] drops = {0, 0.05, 0.15, 0.3};
                    double[] dups = {0, 0.05, 0.2};
                    long[] maxDelays = {1, 3, 10};
                    c.net.setDropRate(drops[rnd.nextInt(drops.length)]);
                    c.net.setDuplicateRate(dups[rnd.nextInt(dups.length)]);
                    c.net.setDelayRange(1, maxDelays[rnd.nextInt(maxDelays.length)]);
                }
                c.runFor(1 + rnd.nextInt(8));
                violations.addAll(checkContinuously(c, round));
            }
            if (violations.isEmpty()) {
                settle(c, violations);
            }
            if (violations.isEmpty()) {
                violations.addAll(checkFinal(c));
            }
        } catch (RuntimeException e) {
            violations.add("exception: " + e);
        }
        int committed = 0;
        for (LogEntry entry : c.appliedEntries().values()) {
            if (!entry.isNoop()) {
                committed++;
            }
        }
        return new Result(seed, size, List.copyOf(violations), proposals, committed, crashes, partitions,
                c.electedTerms(), c.snapshotsTaken(), c.snapshotInstalls());
    }

    /** Heals every fault, restarts every node, and proves the cluster still makes progress. */
    private static void settle(TestCluster c, List<String> violations) {
        c.net.heal();
        c.net.setDropRate(0);
        c.net.setDuplicateRate(0);
        c.net.setDelayRange(1, 1);
        for (NodeId id : c.members()) {
            if (!c.isUp(id)) {
                c.restart(id);
            }
        }
        c.runFor(300);
        try {
            c.awaitSingleLeader(500);
        } catch (AssertionError e) {
            violations.add("liveness: no single leader 500 ticks after healing");
            return;
        }
        long index = c.propose("final-marker");
        if (index < 0) {
            violations.add("liveness: no leader accepted the final command");
            return;
        }
        for (int i = 0; i < 500 && !allCommitted(c, index); i++) {
            c.tick();
        }
        if (!allCommitted(c, index)) {
            violations.add("liveness: final command not committed on every node 500 ticks after healing");
        }
    }

    private static boolean allCommitted(TestCluster c, long index) {
        for (NodeId id : c.members()) {
            if (c.node(id).commitIndex() < index) {
                return false;
            }
        }
        return true;
    }

    /** Cheap checks run after every round while faults are active. */
    static List<String> checkContinuously(TestCluster c, int round) {
        List<String> bad = new ArrayList<>();
        if (!c.electionSafetyViolations().isEmpty()) {
            bad.add("two leaders in one term: " + c.electionSafetyViolations());
        }
        bad.addAll(c.stateMachineViolations());
        Map<Long, LogEntry> applied = c.appliedEntries();
        for (RaftNode leader : c.leaders()) {
            for (Map.Entry<Long, LogEntry> e : applied.entrySet()) {
                LogEntry entry = e.getValue();
                if (entry.term() > leader.currentTerm()) {
                    continue;
                }
                long index = e.getKey();
                if (index <= leader.log().snapshotIndex()) {
                    continue; // inside the leader's snapshot
                }
                if (index > leader.log().lastIndex() || !leader.log().get(index).equals(entry)) {
                    bad.add("leader completeness: " + leader.id() + " (term " + leader.currentTerm()
                            + ") lacks committed entry " + index + " " + entry);
                    break;
                }
            }
        }
        if (round % 25 == 0) {
            bad.addAll(c.logMatchingViolations());
        }
        return bad;
    }

    /** Run once the cluster has healed and converged. */
    static List<String> checkFinal(TestCluster c) {
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
            String command = e.getValue().command();
            if (command != null && !seenCommands.add(command)) {
                bad.add("command applied twice: " + command);
            }
        }
        for (NodeId id : c.members()) {
            RaftLog log = c.log(id);
            for (Map.Entry<Long, LogEntry> e : applied.entrySet()) {
                long index = e.getKey();
                if (index <= log.snapshotIndex()) {
                    continue; // compacted into a snapshot, checked through the state digest below
                }
                if (index > log.lastIndex() || !log.get(index).equals(e.getValue())) {
                    bad.add("lost acknowledged entry " + index + " on " + id);
                    break;
                }
            }
        }
        Set<Long> commitIndexes = new TreeSet<>();
        for (NodeId id : c.members()) {
            commitIndexes.add(c.node(id).commitIndex());
        }
        if (commitIndexes.size() > 1) {
            bad.add("nodes disagree on commit index after healing: " + commitIndexes);
        }
        for (NodeId id : c.members()) {
            String wantState = SnapshotRecorder.expected(applied, c.node(id).lastApplied());
            if (!c.stateOf(id).equals(wantState)) {
                bad.add("state machine on " + id + " diverged: " + c.stateOf(id) + " but expected " + wantState);
            }
        }
        return bad;
    }
}
