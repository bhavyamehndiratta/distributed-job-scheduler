package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.raft.LogEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class SchedulerStateMachineTest {

    private static final long MINUTE = 60_000;

    private Outcome last;
    private final SchedulerStateMachine sm = new SchedulerStateMachine(1_000, (i, c, o) -> last = o);
    private long index = 0;

    private Outcome apply(Command c) {
        return applyAt(++index, c);
    }

    private Outcome applyAt(long at, Command c) {
        index = Math.max(index, at);
        sm.apply(at, new LogEntry(1, CommandCodec.encode(c)));
        return last;
    }

    private void submitCron(String id, int maxAttempts) {
        assertEquals(Outcome.APPLIED, apply(new Command.SubmitJob(id, JobSpec.recurring("h", "", "* * * * *", maxAttempts), null, 0)));
    }

    private void submitOnce(String id, long runAt, int maxAttempts) {
        assertEquals(Outcome.APPLIED, apply(new Command.SubmitJob(id, JobSpec.once("h", "", runAt, maxAttempts), null, 0)));
    }

    private String fireAndGetRunId(String jobId, long time) {
        assertEquals(Outcome.APPLIED, apply(new Command.FireRun(jobId, time, 0)));
        return jobId + "@" + time;
    }

    // ---- submission and idempotency ----

    @Test
    void submitCreatesAnActiveJob() {
        submitOnce("j1", 5_000, 3);
        JobView job = sm.job("j1").orElseThrow();
        assertEquals(JobStatus.ACTIVE, job.status());
        assertEquals(-1, job.lastFiredTime());
        assertTrue(sm.job("nope").isEmpty());
    }

    @Test
    void sameIdempotencyKeyReturnsTheExistingJobInsteadOfCreatingAnother() {
        assertEquals(Outcome.APPLIED, apply(new Command.SubmitJob("a", JobSpec.once("h", "", 1, 1), "k", 0)));
        assertEquals(Outcome.DUPLICATE, apply(new Command.SubmitJob("b", JobSpec.once("h", "", 1, 1), "k", 10)));
        assertEquals(1, sm.jobs().size());
        assertEquals("a", sm.jobIdForKey("k").orElseThrow());
    }

    @Test
    void idempotencyKeysExpireAfterTheRetentionWindow() {
        apply(new Command.SubmitJob("a", JobSpec.once("h", "", 1, 1), "k", 0));
        assertEquals(Outcome.DUPLICATE, apply(new Command.SubmitJob("b", JobSpec.once("h", "", 1, 1), "k", 999)));
        assertEquals(Outcome.APPLIED, apply(new Command.SubmitJob("c", JobSpec.once("h", "", 1, 1), "k", 1_000)));
        assertEquals(2, sm.jobs().size());
        assertEquals("c", sm.jobIdForKey("k").orElseThrow());
    }

    @Test
    void sameJobIdTwiceIsADuplicate() {
        submitOnce("j", 1, 1);
        assertEquals(Outcome.DUPLICATE, apply(new Command.SubmitJob("j", JobSpec.once("other", "", 9, 1), null, 5)));
        assertEquals("h", sm.job("j").orElseThrow().spec().handler());
    }

    // ---- firing ----

    @Test
    void fireCreatesARunWithADeterministicId() {
        submitCron("j", 1);
        String runId = fireAndGetRunId("j", MINUTE);
        assertEquals("j@60000", runId);
        RunView run = sm.run(runId).orElseThrow();
        assertEquals(RunState.SCHEDULED, run.state());
        assertEquals(0, run.token());
    }

    @Test
    void aFireAtOrBeforeTheLatestFiredTimeIsIgnored() {
        submitCron("j", 1);
        fireAndGetRunId("j", 2 * MINUTE);
        assertEquals(Outcome.DUPLICATE, apply(new Command.FireRun("j", 2 * MINUTE, 0)));
        assertEquals(Outcome.DUPLICATE, apply(new Command.FireRun("j", MINUTE, 0)));
        assertEquals(1, sm.runs().size());
        assertEquals(Outcome.APPLIED, apply(new Command.FireRun("j", 3 * MINUTE, 0)));
        assertEquals(2, sm.runs().size());
    }

    @Test
    void aOneShotJobOnlyFiresAtItsOwnTime() {
        submitOnce("j", 5_000, 1);
        assertEquals(Outcome.REJECTED, apply(new Command.FireRun("j", 6_000, 0)));
        assertEquals(Outcome.APPLIED, apply(new Command.FireRun("j", 5_000, 0)));
        assertEquals(Outcome.DUPLICATE, apply(new Command.FireRun("j", 5_000, 0)));
    }

    @Test
    void aCronFireMustMatchTheExpression() {
        assertEquals(Outcome.APPLIED, apply(new Command.SubmitJob("j", JobSpec.recurring("h", "", "0 * * * *", 1), null, 0)));
        assertEquals(Outcome.REJECTED, apply(new Command.FireRun("j", MINUTE, 0)));
        assertEquals(Outcome.APPLIED, apply(new Command.FireRun("j", 3_600_000, 0)));
    }

    @Test
    void firingAnUnknownJobIsReportedNotFatal() {
        assertEquals(Outcome.UNKNOWN, apply(new Command.FireRun("ghost", 1, 0)));
    }

    // ---- assignment, tokens, completion ----

    @Test
    void theTokenIsTheLogIndexOfTheAssignEntry() {
        submitOnce("j", 1, 3);
        String run = fireAndGetRunId("j", 1);
        assertEquals(Outcome.APPLIED, applyAt(50, new Command.AssignRun(run, "w1", 10)));
        RunView v = sm.run(run).orElseThrow();
        assertEquals(RunState.ASSIGNED, v.state());
        assertEquals(50, v.token());
        assertEquals("w1", v.workerId());
        assertEquals(1, v.attempts());
    }

    @Test
    void aRunCanOnlyBeAssignedWhileScheduled() {
        submitOnce("j", 1, 3);
        String run = fireAndGetRunId("j", 1);
        assertEquals(Outcome.APPLIED, apply(new Command.AssignRun(run, "w1", 10)));
        assertEquals(Outcome.REJECTED, apply(new Command.AssignRun(run, "w2", 10)));
        assertEquals(Outcome.UNKNOWN, apply(new Command.AssignRun("ghost", "w2", 10)));
    }

    @Test
    void aStaleWorkerCannotChangeARunThatWasReassigned() {
        submitOnce("j", 1, 3);
        String run = fireAndGetRunId("j", 1);
        applyAt(10, new Command.AssignRun(run, "slow", 5));
        assertEquals(Outcome.APPLIED, apply(new Command.ExpireAssignment(run, 10)));
        assertEquals(RunState.SCHEDULED, sm.run(run).orElseThrow().state());
        applyAt(20, new Command.AssignRun(run, "fast", 5));

        // the slow worker wakes up and reports with its old token
        assertEquals(Outcome.STALE_TOKEN, apply(new Command.RunStarted(run, 10)));
        assertEquals(Outcome.STALE_TOKEN, apply(new Command.RunCompleted(run, 10, true, "late")));
        assertEquals(Outcome.STALE_TOKEN, apply(new Command.ExpireAssignment(run, 10)));
        RunView v = sm.run(run).orElseThrow();
        assertEquals(RunState.ASSIGNED, v.state());
        assertEquals(20, v.token());
        assertEquals("fast", v.workerId());

        assertEquals(Outcome.APPLIED, apply(new Command.RunCompleted(run, 20, true, "ok")));
        assertEquals(RunState.SUCCEEDED, sm.run(run).orElseThrow().state());
    }

    @Test
    void startedNeedsTheCurrentTokenAndIsIdempotent() {
        submitOnce("j", 1, 1);
        String run = fireAndGetRunId("j", 1);
        applyAt(7, new Command.AssignRun(run, "w", 5));
        assertEquals(Outcome.STALE_TOKEN, apply(new Command.RunStarted(run, 6)));
        assertEquals(Outcome.APPLIED, apply(new Command.RunStarted(run, 7)));
        assertEquals(RunState.RUNNING, sm.run(run).orElseThrow().state());
        assertEquals(Outcome.DUPLICATE, apply(new Command.RunStarted(run, 7)));
    }

    @Test
    void aRepeatedCompletionChangesNothing() {
        submitOnce("j", 1, 3);
        String run = fireAndGetRunId("j", 1);
        applyAt(9, new Command.AssignRun(run, "w", 5));
        assertEquals(Outcome.APPLIED, apply(new Command.RunCompleted(run, 9, true, "done")));
        String before = sm.digest();
        assertEquals(Outcome.DUPLICATE, apply(new Command.RunCompleted(run, 9, true, "done")));
        assertEquals(Outcome.DUPLICATE, apply(new Command.RunCompleted(run, 9, false, "contradiction")));
        assertEquals(before, sm.digest());
    }

    @Test
    void aFailedRunIsRetriedUntilItRunsOutOfAttempts() {
        submitOnce("j", 1, 2);
        String run = fireAndGetRunId("j", 1);
        applyAt(10, new Command.AssignRun(run, "w", 5));
        assertEquals(Outcome.APPLIED, apply(new Command.RunCompleted(run, 10, false, "boom")));
        RunView afterFirst = sm.run(run).orElseThrow();
        assertEquals(RunState.SCHEDULED, afterFirst.state());
        assertEquals(0, afterFirst.token());
        assertEquals(1, afterFirst.attempts());

        applyAt(20, new Command.AssignRun(run, "w", 5));
        assertEquals(Outcome.APPLIED, apply(new Command.RunCompleted(run, 20, false, "boom again")));
        assertEquals(RunState.FAILED, sm.run(run).orElseThrow().state());
        assertEquals(Outcome.REJECTED, apply(new Command.AssignRun(run, "w", 5)));
    }

    @Test
    void anExpiredLeaseCountsAsAFailedAttempt() {
        submitOnce("j", 1, 2);
        String run = fireAndGetRunId("j", 1);
        applyAt(10, new Command.AssignRun(run, "w", 5));
        assertEquals(Outcome.APPLIED, apply(new Command.ExpireAssignment(run, 10)));
        assertEquals(RunState.SCHEDULED, sm.run(run).orElseThrow().state());
        applyAt(20, new Command.AssignRun(run, "w", 5));
        assertEquals(Outcome.APPLIED, apply(new Command.ExpireAssignment(run, 20)));
        assertEquals(RunState.FAILED, sm.run(run).orElseThrow().state());
        assertEquals("lease expired", sm.run(run).orElseThrow().detail());
    }

    // ---- cancellation ----

    @Test
    void cancelStopsFurtherRunsAndCancelsQueuedOnes() {
        submitCron("j", 1);
        String run = fireAndGetRunId("j", MINUTE);
        assertEquals(Outcome.APPLIED, apply(new Command.CancelJob("j")));
        assertEquals(RunState.CANCELLED, sm.run(run).orElseThrow().state());
        assertEquals(Outcome.REJECTED, apply(new Command.FireRun("j", 2 * MINUTE, 0)));
        assertEquals(Outcome.REJECTED, apply(new Command.AssignRun(run, "w", 5)));
        assertEquals(Outcome.DUPLICATE, apply(new Command.CancelJob("j")));
        assertEquals(Outcome.UNKNOWN, apply(new Command.CancelJob("ghost")));
        assertTrue(sm.dueFires(10 * MINUTE).isEmpty());
    }

    @Test
    void aRunAlreadyInFlightWhenTheJobIsCancelledFinishesButIsNotRetried() {
        submitOnce("j", 1, 5);
        String run = fireAndGetRunId("j", 1);
        applyAt(10, new Command.AssignRun(run, "w", 5));
        apply(new Command.CancelJob("j"));
        assertEquals(RunState.ASSIGNED, sm.run(run).orElseThrow().state());
        assertEquals(Outcome.APPLIED, apply(new Command.RunCompleted(run, 10, false, "stopped")));
        assertEquals(RunState.CANCELLED, sm.run(run).orElseThrow().state());
    }

    // ---- misfire policy ----

    @Test
    void afterAnOutageOnlyTheMostRecentMissedFireTimeIsFired() {
        submitCron("j", 1); // every minute, created at time 0
        long now = 10 * MINUTE + 5_000;
        List<Command.FireRun> due = sm.dueFires(now);
        assertEquals(List.of(new Command.FireRun("j", 10 * MINUTE, 9)), due);
        apply(due.get(0));
        assertEquals(9, sm.job("j").orElseThrow().skippedCount());
        assertEquals(1, sm.runs().size());
        assertTrue(sm.dueFires(now).isEmpty());
        assertEquals(List.of(new Command.FireRun("j", 11 * MINUTE, 0)), sm.dueFires(11 * MINUTE));
    }

    @Test
    void aOneShotJobWhoseTimePassedStillFiresOnce() {
        submitOnce("j", 1_000, 1);
        assertTrue(sm.dueFires(999).isEmpty());
        List<Command.FireRun> due = sm.dueFires(5_000_000);
        assertEquals(List.of(new Command.FireRun("j", 1_000, 0)), due);
        apply(due.get(0));
        assertTrue(sm.dueFires(5_000_000).isEmpty());
    }

    // ---- robustness and determinism ----

    @Test
    void anUndecodableEntryIsRejectedWithoutChangingState() {
        submitOnce("j", 1, 1);
        String before = sm.digest();
        sm.apply(99, new LogEntry(1, "garbage"));
        assertEquals(Outcome.MALFORMED, last);
        sm.apply(100, new LogEntry(1, "F|j|notanumber|0"));
        assertEquals(Outcome.MALFORMED, last);
        sm.apply(101, LogEntry.noop(1));
        assertEquals(before, sm.digest());
    }

    private static List<Command> randomCommands(long seed, SchedulerStateMachine probe, List<Long> indexes) {
        // Generates commands against a probe machine so ids and tokens are mostly meaningful.
        Random rnd = new Random(seed);
        List<Command> out = new ArrayList<>();
        long index = 0;
        for (int step = 0; step < 300; step++) {
            List<RunView> runs = probe.runs();
            List<JobView> jobs = probe.jobs();
            Command c;
            int action = rnd.nextInt(10);
            if (jobs.isEmpty() || action == 0) {
                String id = "job" + rnd.nextInt(6);
                JobSpec spec = rnd.nextBoolean()
                        ? JobSpec.recurring("h", "", "*/" + (1 + rnd.nextInt(5)) + " * * * *", 1 + rnd.nextInt(3))
                        : JobSpec.once("h", "", rnd.nextInt(10) * MINUTE, 1 + rnd.nextInt(3));
                c = new Command.SubmitJob(id, spec, rnd.nextInt(3) == 0 ? "k" + rnd.nextInt(3) : null, rnd.nextInt(5000));
            } else if (action == 1) {
                c = new Command.CancelJob("job" + rnd.nextInt(6));
            } else if (action <= 3) {
                String id = jobs.get(rnd.nextInt(jobs.size())).jobId();
                c = new Command.FireRun(id, rnd.nextInt(12) * MINUTE, rnd.nextInt(3));
            } else if (runs.isEmpty()) {
                continue;
            } else {
                RunView r = runs.get(rnd.nextInt(runs.size()));
                long token = rnd.nextInt(4) == 0 ? 1 + rnd.nextInt(400) : Math.max(1, r.token());
                switch (action) {
                    case 4, 5 -> c = new Command.AssignRun(r.runId(), "w" + rnd.nextInt(3), 1 + rnd.nextInt(10));
                    case 6 -> c = new Command.RunStarted(r.runId(), token);
                    case 7 -> c = new Command.RunCompleted(r.runId(), token, rnd.nextBoolean(), "d");
                    default -> c = new Command.ExpireAssignment(r.runId(), token);
                }
            }
            out.add(c);
            index += 1 + rnd.nextInt(3);
            indexes.add(index);
            probe.apply(index, new LogEntry(1, CommandCodec.encode(c)));
        }
        return out;
    }

    @Test
    void randomCommandStreamsKeepTheInvariantsAndReplayIdentically() {
        for (long seed = 1; seed <= 200; seed++) {
            List<Long> indexes = new ArrayList<>();
            List<Command> commands = randomCommands(seed, new SchedulerStateMachine(2_000, (i, c, o) -> { }), indexes);
            SchedulerStateMachine a = new SchedulerStateMachine(2_000, (i, c, o) -> { });
            SchedulerStateMachine b = new SchedulerStateMachine(2_000, (i, c, o) -> { });
            Map<String, RunState> previous = new HashMap<>();
            Map<String, Long> maxToken = new HashMap<>();
            for (int n = 0; n < commands.size(); n++) {
                Command c = commands.get(n);
                long at = indexes.get(n);
                a.apply(at, new LogEntry(1, CommandCodec.encode(c)));
                b.apply(at, new LogEntry(1, CommandCodec.encode(c)));
                for (RunView r : a.runs()) {
                    RunState before = previous.get(r.runId());
                    if (before != null && before.isTerminal()) {
                        assertEquals(before, r.state(), "seed " + seed + ": terminal run changed " + r);
                    }
                    previous.put(r.runId(), r.state());
                    JobView job = a.job(r.jobId()).orElseThrow();
                    assertTrue(r.attempts() <= job.spec().maxAttempts(), "seed " + seed + ": " + r);
                    if (r.state() == RunState.ASSIGNED || r.state() == RunState.RUNNING) {
                        assertTrue(r.token() > 0 && r.workerId() != null, "seed " + seed + ": " + r);
                        long max = maxToken.getOrDefault(r.runId(), 0L);
                        assertTrue(r.token() >= max, "seed " + seed + ": token went backwards " + r);
                        maxToken.put(r.runId(), r.token());
                    }
                    if (r.state() == RunState.SCHEDULED) {
                        assertEquals(0, r.token(), "seed " + seed + ": " + r);
                    }
                }
            }
            assertEquals(a.digest(), b.digest(), "seed " + seed + ": replicas diverged");
        }
    }

    @Test
    void replicasThatApplyTheSameEntriesAgree() {
        SchedulerStateMachine x = new SchedulerStateMachine();
        SchedulerStateMachine y = new SchedulerStateMachine();
        List<Command> commands = List.of(
                new Command.SubmitJob("j", JobSpec.recurring("h", "", "* * * * *", 2), "k", 0),
                new Command.FireRun("j", MINUTE, 0),
                new Command.AssignRun("j@60000", "w", 5),
                new Command.RunCompleted("j@60000", 3, true, "ok"));
        long i = 0;
        for (Command c : commands) {
            i++;
            x.apply(i, new LogEntry(1, CommandCodec.encode(c)));
            y.apply(i, new LogEntry(1, CommandCodec.encode(c)));
        }
        assertEquals(x.digest(), y.digest());
        assertEquals(RunState.SUCCEEDED, x.run("j@60000").orElseThrow().state());
        assertFalse(x.digest().isEmpty());
    }
}
