package io.github.bhavyamehndiratta.djs.scheduler;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Seeded chaos for the whole scheduler: nodes crash and restart, the network
 * partitions, drops, duplicates and delays messages, workers crash or freeze
 * for long stretches, and clients keep submitting jobs. A scenario is fully
 * determined by its seed, so any failure replays exactly.
 *
 * <p>Checked properties:
 * <ul>
 *   <li>at most one leader per term;</li>
 *   <li>committed logs agree: every node applies the same command with the same outcome at each index;</li>
 *   <li>no duplicate effect: no run id is ever applied to the outside world twice;</li>
 *   <li>no lost acked job: a job is acked once any node has applied it, and after healing every
 *       acked job must still exist and have run to success;</li>
 *   <li>replicas converge to identical state after healing;</li>
 *   <li>reads are linearizable: a GetJob sent after a job was acked must find it, and after a run
 *       succeeded on some node must show it succeeded, whichever node answers (a deposed or
 *       partitioned leader must never answer from stale state).</li>
 * </ul>
 */
final class SchedulerChaosHarness {

    record Result(long seed, int nodes, List<String> violations, int submitted, int acked, int succeeded,
                  int duplicateEffects, int nodeCrashes, int workerCrashes, int pauses, int partitions,
                  int snapshotsTaken, int snapshotInstalls, int readsSent, int readsAnswered) {
        boolean ok() {
            return violations.isEmpty();
        }
    }

    private static final int WORKERS = 3;

    private SchedulerChaosHarness() {
    }

    static Result run(long seed) {
        return run(seed, true);
    }

    /** {@code protectedStore=false} uses an effect store with no dedupe or fencing, to prove the checker sees duplicates. */
    static Result run(long seed, boolean protectedStore) {
        Random rnd = new Random(seed * 2_654_435_761L + 29);
        int size = rnd.nextBoolean() ? 3 : 5;
        Random snapshotRandom = new Random(seed ^ 0x5bd1e995L);
        int threshold = snapshotRandom.nextInt(3) == 0 ? 0 : 5 + snapshotRandom.nextInt(20);
        SchedulerCluster c = new SchedulerCluster(size, WORKERS, seed, new EffectStore(protectedStore), threshold);
        List<String> violations = new ArrayList<>();
        Map<String, JobSpec> specs = new LinkedHashMap<>();
        Map<String, Long> lastTry = new HashMap<>();
        Set<String> acked = new TreeSet<>();
        Set<NodeId> workersDown = new TreeSet<>();
        int submitted = 0;
        int nodeCrashes = 0;
        int workerCrashes = 0;
        int pauses = 0;
        int partitions = 0;
        int succeeded = 0;
        Random readRnd = new Random(seed ^ 0x2545F491L);
        NodeId probe = new NodeId("probe");
        Map<Long, ReadExpect> expects = new HashMap<>();
        int[] readCounts = new int[2]; // sent, answered
        long[] nextRid = {1};
        List<String> ackOrder = new ArrayList<>();
        c.net.register(probe, (from, msg) -> onProbeReply(msg, expects, readCounts, violations));
        c.net.setNodeUp(probe, true);
        try {
            int rounds = 400 + rnd.nextInt(400);
            for (int round = 0; round < rounds && violations.isEmpty(); round++) {
                int action = rnd.nextInt(100);
                if (action < 35) {
                    SchedulerNode leader = c.leader();
                    if (leader != null) {
                        String jobId = "j" + seed + "-" + submitted;
                        long runAt = c.nowMillis() + rnd.nextInt(3000);
                        JobSpec spec = JobSpec.once("effect", String.valueOf(5 + rnd.nextInt(40)), runAt, 20);
                        if (leader.submitJob(jobId, spec, "key-" + jobId) > 0) {
                            specs.put(jobId, spec);
                            lastTry.put(jobId, c.net.now());
                            submitted++;
                        }
                    }
                } else if (action < 42) {
                    if (downCount(c) < (size - 1) / 2) {
                        List<NodeId> up = upNodes(c);
                        c.crashNode(up.get(rnd.nextInt(up.size())));
                        nodeCrashes++;
                    }
                } else if (action < 46) {
                    SchedulerNode leader = c.leader();
                    if (leader != null && downCount(c) < (size - 1) / 2) {
                        c.crashNode(leader.id());
                        nodeCrashes++;
                    }
                } else if (action < 56) {
                    List<NodeId> down = new ArrayList<>();
                    for (NodeId id : c.nodeIds()) {
                        if (!c.isNodeUp(id)) {
                            down.add(id);
                        }
                    }
                    if (!down.isEmpty()) {
                        c.restartNode(down.get(rnd.nextInt(down.size())));
                    }
                } else if (action < 62) {
                    List<NodeId> ids = new ArrayList<>(c.nodeIds());
                    Collections.shuffle(ids, rnd);
                    int cut = 1 + rnd.nextInt(size - 1);
                    Set<NodeId> a = new HashSet<>(ids.subList(0, cut));
                    Set<NodeId> b = new HashSet<>(ids.subList(cut, size));
                    for (NodeId w : c.workerIds()) {
                        (rnd.nextBoolean() ? a : b).add(w);
                    }
                    SchedulerNode victim = c.leader();
                    if (victim != null && readRnd.nextInt(100) < 40) {
                        // Cut the current leader off together with the probe: the probe can still ask the
                        // deposed leader while the majority elects a new one and keeps committing.
                        a.clear();
                        b.clear();
                        a.add(victim.id());
                        for (NodeId id : c.nodeIds()) {
                            if (!id.equals(victim.id())) {
                                b.add(id);
                            }
                        }
                        for (NodeId w : c.workerIds()) {
                            (readRnd.nextBoolean() ? a : b).add(w);
                        }
                        a.add(probe);
                        c.stallNode(victim.id(), 20 + readRnd.nextInt(80));
                    } else {
                        (readRnd.nextBoolean() ? a : b).add(probe);
                    }
                    c.net.partition(a, b);
                    partitions++;
                } else if (action < 68) {
                    c.net.heal();
                } else if (action < 74) {
                    List<NodeId> up = upWorkers(c, workersDown);
                    if (!up.isEmpty()) {
                        NodeId w = up.get(rnd.nextInt(up.size()));
                        c.crashWorker(w);
                        workersDown.add(w);
                        workerCrashes++;
                    }
                } else if (action < 82) {
                    if (!workersDown.isEmpty()) {
                        NodeId w = new ArrayList<>(workersDown).get(rnd.nextInt(workersDown.size()));
                        c.restartWorker(w);
                        workersDown.remove(w);
                    }
                } else if (action < 90) {
                    List<NodeId> up = upWorkers(c, workersDown);
                    if (!up.isEmpty()) {
                        c.pauseWorker(up.get(rnd.nextInt(up.size())), 10 + rnd.nextInt(190));
                        pauses++;
                    }
                } else if (action < 94) {
                    double[] drops = {0, 0.05, 0.15};
                    double[] dups = {0, 0.05, 0.2};
                    long[] maxDelays = {1, 3, 10};
                    c.net.setDropRate(drops[rnd.nextInt(drops.length)]);
                    c.net.setDuplicateRate(dups[rnd.nextInt(dups.length)]);
                    c.net.setDelayRange(1, maxDelays[rnd.nextInt(maxDelays.length)]);
                }
                c.runFor(1 + rnd.nextInt(8));
                trackAcks(c, specs, lastTry, acked, true);
                issueReads(c, readRnd, probe, acked, specs, expects, readCounts, nextRid, ackOrder);
                checkContinuously(c, violations, round);
            }
            if (violations.isEmpty()) {
                settle(c, workersDown, specs, lastTry, acked, violations);
            }
            if (violations.isEmpty()) {
                succeeded = checkFinal(c, specs, acked, violations);
            }
        } catch (RuntimeException e) {
            violations.add("exception: " + e);
        }
        return new Result(seed, size, List.copyOf(violations), submitted, acked.size(), succeeded,
                c.store.duplicateEffects().size(), nodeCrashes, workerCrashes, pauses, partitions,
                c.snapshotsTaken(), c.snapshotInstalls(), readCounts[0], readCounts[1]);
    }

    private record ReadExpect(String jobId, String runId, boolean mustSucceed) {
    }

    /** Sometimes sends a read for an acked job to every up node, remembering what a fresh answer must show. */
    private static void issueReads(SchedulerCluster c, Random readRnd, NodeId probe, Set<String> acked,
                                   Map<String, JobSpec> specs, Map<Long, ReadExpect> expects, int[] counts,
                                   long[] nextRid, List<String> ackOrder) {
        for (String a : acked) {
            if (!ackOrder.contains(a)) {
                ackOrder.add(a);
            }
        }
        if (ackOrder.isEmpty() || readRnd.nextInt(100) >= 80) {
            return;
        }
        int total = ackOrder.size();
        // Mostly read one of the newest acked jobs: that is what a stale leader is most likely to miss.
        String jobId = readRnd.nextInt(100) < 70
                ? ackOrder.get(total - 1 - readRnd.nextInt(Math.min(3, total)))
                : ackOrder.get(readRnd.nextInt(total));
        String run = runId(jobId, specs);
        boolean done = false;
        for (NodeId n : c.nodeIds()) {
            if (c.isNodeUp(n)) {
                RunView r = c.node(n).stateMachine().run(run).orElse(null);
                if (r != null && r.state() == RunState.SUCCEEDED) {
                    done = true;
                    break;
                }
            }
        }
        long rid = nextRid[0]++;
        expects.put(rid, new ReadExpect(jobId, run, done));
        for (NodeId n : c.nodeIds()) {
            if (c.isNodeUp(n)) {
                c.net.send(probe, n, new ClientMessages.GetJob(rid, jobId));
                counts[0]++;
            }
        }
    }

    private static void onProbeReply(Message msg, Map<Long, ReadExpect> expects, int[] counts,
                                     List<String> violations) {
        if (msg instanceof ClientMessages.JobReply(long rid, boolean found, JobView job, List<RunView> runs)) {
            counts[1]++;
            ReadExpect e = expects.get(rid);
            if (e == null) {
                return;
            }
            if (!found) {
                violations.add("stale read: acked job " + e.jobId() + " reported missing");
            } else if (e.mustSucceed()) {
                boolean ok = false;
                for (RunView r : runs) {
                    if (r.runId().equals(e.runId()) && r.state() == RunState.SUCCEEDED) {
                        ok = true;
                    }
                }
                if (!ok) {
                    violations.add("stale read: run " + e.runId() + " had already succeeded but a read did not show it");
                }
            }
        }
    }

    private static int downCount(SchedulerCluster c) {
        int down = 0;
        for (NodeId id : c.nodeIds()) {
            if (!c.isNodeUp(id)) {
                down++;
            }
        }
        return down;
    }

    private static List<NodeId> upNodes(SchedulerCluster c) {
        List<NodeId> up = new ArrayList<>();
        for (NodeId id : c.nodeIds()) {
            if (c.isNodeUp(id)) {
                up.add(id);
            }
        }
        return up;
    }

    private static List<NodeId> upWorkers(SchedulerCluster c, Set<NodeId> workersDown) {
        List<NodeId> up = new ArrayList<>();
        for (NodeId id : c.workerIds()) {
            if (!workersDown.contains(id)) {
                up.add(id);
            }
        }
        return up;
    }

    /** A job is acked once any running node has applied it, which means it is committed. Resubmits stragglers. */
    private static void trackAcks(SchedulerCluster c, Map<String, JobSpec> specs, Map<String, Long> lastTry,
                                  Set<String> acked, boolean resubmit) {
        for (Map.Entry<String, JobSpec> e : specs.entrySet()) {
            String id = e.getKey();
            if (acked.contains(id)) {
                continue;
            }
            boolean applied = false;
            for (NodeId n : c.nodeIds()) {
                if (c.isNodeUp(n) && c.node(n).stateMachine().job(id).isPresent()) {
                    applied = true;
                    break;
                }
            }
            if (applied) {
                acked.add(id);
            } else if (resubmit && c.net.now() - lastTry.get(id) > 300) {
                SchedulerNode leader = c.leader();
                if (leader != null && leader.submitJob(id, e.getValue(), "key-" + id) > 0) {
                    lastTry.put(id, c.net.now());
                }
            }
        }
    }

    private static void checkContinuously(SchedulerCluster c, List<String> violations, int round) {
        Map<Long, Set<NodeId>> bad = c.electionSafetyViolations();
        if (!bad.isEmpty()) {
            violations.add("round " + round + ": two leaders in one term: " + bad);
        }
        if (!c.appliedViolations.isEmpty()) {
            violations.add("round " + round + ": committed logs disagree: " + c.appliedViolations.get(0));
        }
        Map<String, Integer> dup = c.store.duplicateEffects();
        if (!dup.isEmpty()) {
            violations.add("round " + round + ": duplicate effect: " + dup);
        }
    }

    /** Heals every fault, restarts everything, and waits for every acked job to finish. */
    private static void settle(SchedulerCluster c, Set<NodeId> workersDown, Map<String, JobSpec> specs,
                               Map<String, Long> lastTry, Set<String> acked, List<String> violations) {
        c.net.heal();
        c.net.setDropRate(0);
        c.net.setDuplicateRate(0);
        c.net.setDelayRange(1, 3);
        for (NodeId id : c.nodeIds()) {
            if (!c.isNodeUp(id)) {
                c.restartNode(id);
            }
        }
        for (NodeId id : new ArrayList<>(workersDown)) {
            c.restartWorker(id);
        }
        workersDown.clear();
        c.runFor(300);
        trackAcks(c, specs, lastTry, acked, false);
        boolean done = c.runUntil(() -> {
            SchedulerNode leader = c.leader();
            if (leader == null) {
                return false;
            }
            for (String id : acked) {
                RunView r = leader.stateMachine().run(runId(id, specs)).orElse(null);
                if (r == null || r.state() != RunState.SUCCEEDED) {
                    return false;
                }
            }
            return true;
        }, 8000);
        if (!done) {
            violations.add("after healing, acked jobs did not all finish: " + unfinished(c, specs, acked));
            return;
        }
        c.runFor(400);
        if (c.distinctDigests().size() != 1) {
            violations.add("replicas disagree after healing: " + c.distinctDigests().size() + " distinct states");
        }
    }

    private static String runId(String jobId, Map<String, JobSpec> specs) {
        return jobId + "@" + specs.get(jobId).runAt();
    }

    private static String unfinished(SchedulerCluster c, Map<String, JobSpec> specs, Set<String> acked) {
        SchedulerNode leader = c.leader();
        List<String> out = new ArrayList<>();
        for (String id : acked) {
            RunView r = leader == null ? null : leader.stateMachine().run(runId(id, specs)).orElse(null);
            if (r == null || r.state() != RunState.SUCCEEDED) {
                out.add(id + ":" + (r == null ? "missing" : r.state() + " attempts=" + r.attempts()));
                if (out.size() >= 5) {
                    break;
                }
            }
        }
        return out.toString();
    }

    /** Returns how many acked jobs ended in SUCCEEDED with exactly one effect. */
    private static int checkFinal(SchedulerCluster c, Map<String, JobSpec> specs, Set<String> acked,
                                  List<String> violations) {
        checkContinuously(c, violations, -1);
        SchedulerNode leader = c.leader();
        if (leader == null) {
            violations.add("no leader after healing");
            return 0;
        }
        int succeeded = 0;
        for (String id : acked) {
            if (leader.stateMachine().job(id).isEmpty()) {
                violations.add("lost acked job " + id);
                continue;
            }
            String run = runId(id, specs);
            RunView r = leader.stateMachine().run(run).orElse(null);
            if (r == null || r.state() != RunState.SUCCEEDED) {
                violations.add("acked job " + id + " did not succeed: " + (r == null ? "no run" : r.state()));
            } else if (c.store.effectCount(run) != 1) {
                violations.add("run " + run + " has " + c.store.effectCount(run) + " effects");
            } else {
                succeeded++;
            }
        }
        return succeeded;
    }
}
