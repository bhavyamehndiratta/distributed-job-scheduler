package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import org.junit.jupiter.api.Test;

class SchedulerClusterTest {

    private static boolean inFlight(RunState s) {
        return s == RunState.ASSIGNED || s == RunState.RUNNING;
    }

    @Test
    void aOneShotJobRunsToSuccessWithExactlyOneEffect() {
        SchedulerCluster c = new SchedulerCluster(3, 2, 1, new EffectStore());
        c.awaitLeader();
        long runAt = c.nowMillis() + 2_000;
        c.submit("j1", JobSpec.once("effect", "10", runAt, 3), null);
        String run = "j1@" + runAt;
        assertTrue(c.runUntil(() -> c.runState(run) == RunState.SUCCEEDED, 800), "state: " + c.runState(run));
        assertEquals(1, c.store.effectCount(run));
        assertEquals(1, c.runView(run).attempts());
        assertTrue(c.electionSafetyViolations().isEmpty());
    }

    @Test
    void aCronJobKeepsFiringAndEachRunHasOneEffect() {
        SchedulerCluster c = new SchedulerCluster(3, 2, 2, new EffectStore());
        c.awaitLeader();
        c.submit("cron1", JobSpec.recurring("effect", "5", "* * * * *", 3), null);
        assertTrue(c.runUntil(() -> c.runState("cron1@60000") == RunState.SUCCEEDED
                && c.runState("cron1@120000") == RunState.SUCCEEDED, 1500));
        for (RunView r : c.leader().stateMachine().runs()) {
            if (r.state() == RunState.SUCCEEDED) {
                assertEquals(1, c.store.effectCount(r.runId()), r.runId());
            }
        }
        assertTrue(c.store.duplicateEffects().isEmpty());
    }

    @Test
    void aCrashedWorkerIsDetectedByItsLeaseAndTheRunIsRetriedElsewhere() {
        SchedulerCluster c = new SchedulerCluster(3, 2, 3, new EffectStore());
        c.awaitLeader();
        long runAt = c.nowMillis() + 1_000;
        c.submit("j", JobSpec.once("effect", "100", runAt, 3), null);
        String run = "j@" + runAt;
        assertTrue(c.runUntil(() -> c.runState(run) != null && inFlight(c.runState(run)), 500));
        c.crashWorker(new NodeId(c.runView(run).workerId()));
        assertTrue(c.runUntil(() -> c.runState(run) == RunState.SUCCEEDED, 3000), "state: " + c.runState(run));
        assertEquals(2, c.runView(run).attempts());
        assertEquals(1, c.store.effectCount(run));
    }

    @Test
    void aPausedWorkerThatWakesUpLateNeverCausesASecondEffect() {
        for (int pause : new int[] {20, 40, 60, 80, 100, 150, 200}) {
            SchedulerCluster c = new SchedulerCluster(3, 2, 100 + pause, new EffectStore());
            c.awaitLeader();
            long runAt = c.nowMillis() + 1_000;
            c.submit("j", JobSpec.once("effect", "40", runAt, 5), null);
            String run = "j@" + runAt;
            assertTrue(c.runUntil(() -> c.runState(run) == RunState.RUNNING, 500), "pause " + pause);
            c.pauseWorker(new NodeId(c.runView(run).workerId()), pause);
            assertTrue(c.runUntil(() -> c.runState(run) == RunState.SUCCEEDED, 3000), "pause " + pause);
            c.runFor(400); // let the paused worker wake up and try to finish
            assertEquals(1, c.store.effectCount(run), "pause " + pause);
            assertTrue(c.store.duplicateEffects().isEmpty(), "pause " + pause);
        }
    }

    @Test
    void aLeaderCrashMidJobStillEndsWithOneEffectAndIdenticalReplicas() {
        SchedulerCluster c = new SchedulerCluster(3, 2, 5, new EffectStore());
        c.awaitLeader();
        long runAt = c.nowMillis() + 1_000;
        c.submit("j", JobSpec.once("effect", "80", runAt, 4), null);
        String run = "j@" + runAt;
        assertTrue(c.runUntil(() -> c.runState(run) == RunState.RUNNING, 500));
        NodeId oldLeader = c.leader().id();
        c.crashNode(oldLeader);
        assertTrue(c.runUntil(() -> c.runState(run) == RunState.SUCCEEDED, 4000), "state: " + c.runState(run));
        assertEquals(1, c.store.effectCount(run));
        c.restartNode(oldLeader);
        c.runFor(400);
        assertEquals(1, c.distinctDigests().size(), "replicas must agree after the old leader rejoins");
        assertTrue(c.electionSafetyViolations().isEmpty());
    }

    @Test
    void submittingTwiceWithTheSameIdempotencyKeyCreatesOneJob() {
        SchedulerCluster c = new SchedulerCluster(3, 1, 6, new EffectStore());
        c.awaitLeader();
        long runAt = c.nowMillis() + 5_000;
        c.submit("first", JobSpec.once("effect", "5", runAt, 1), "client-key");
        c.leader().submitJob("second", JobSpec.once("effect", "5", runAt, 1), "client-key");
        c.runFor(100);
        assertEquals(1, c.leader().stateMachine().jobs().size());
        assertEquals("first", c.leader().stateMachine().jobIdForKey("client-key").orElseThrow());
    }

    @Test
    void aFailingJobIsRetriedThenMarkedFailedWithNoEffect() {
        SchedulerCluster c = new SchedulerCluster(3, 2, 7, new EffectStore());
        c.awaitLeader();
        long runAt = c.nowMillis() + 1_000;
        c.submit("bad", JobSpec.once("fail", "", runAt, 2), null);
        String run = "bad@" + runAt;
        assertTrue(c.runUntil(() -> c.runState(run) == RunState.FAILED, 1500), "state: " + c.runState(run));
        assertEquals(2, c.runView(run).attempts());
        assertEquals(0, c.store.effectCount(run));
    }
}
