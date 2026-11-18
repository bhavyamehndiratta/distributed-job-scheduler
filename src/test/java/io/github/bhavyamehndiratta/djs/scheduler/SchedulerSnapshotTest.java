package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.raft.LogEntry;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SchedulerSnapshotTest {

    private static final String NASTY = "a|b\\c\nd\te\u00e9\u4e2d";

    private final List<String> outcomesA = new ArrayList<>();
    private final List<String> outcomesB = new ArrayList<>();
    private final SchedulerStateMachine sm = new SchedulerStateMachine(
            SchedulerStateMachine.DEFAULT_IDEMPOTENCY_RETENTION_MILLIS,
            (i, c, o) -> outcomesA.add(i + ":" + c + "=>" + o));
    private long index = 0;

    private static LogEntry entry(Command c) {
        return new LogEntry(1, CommandCodec.encode(c));
    }

    private long ap(Command c) {
        sm.apply(++index, entry(c));
        return index;
    }

    private void populate() {
        ap(new Command.SubmitJob("j1", JobSpec.once("effect", NASTY, 1000, 3), "k1\tx", 10));
        ap(new Command.SubmitJob("j2", JobSpec.once("effect", "5", 2000, 3), null, 11));
        ap(new Command.SubmitJob("j3", JobSpec.once("effect", "5", 3000, 3), "k3", 12));
        ap(new Command.SubmitJob("j4", JobSpec.once("effect", "5", 4000, 3), "k4", 13));
        ap(new Command.FireRun("j1", 1000, 0));
        long a1 = ap(new Command.AssignRun("j1@1000", "w\n1", 30));
        ap(new Command.RunStarted("j1@1000", a1));
        ap(new Command.RunCompleted("j1@1000", a1, true, NASTY));
        ap(new Command.FireRun("j2", 2000, 0));
        ap(new Command.AssignRun("j2@2000", "w2", 30));
        ap(new Command.CancelJob("j3"));
        ap(new Command.FireRun("j4", 4000, 0));
        long a4 = ap(new Command.AssignRun("j4@4000", "w3", 30));
        ap(new Command.ExpireAssignment("j4@4000", a4));
    }

    @Test
    void snapshotAndRestoreReproduceTheWholeState() {
        populate();
        String snap = sm.snapshot();
        SchedulerStateMachine fresh = new SchedulerStateMachine();
        fresh.restore(snap);
        assertEquals(sm.digest(), fresh.digest());
        assertEquals(snap, fresh.snapshot());
        assertEquals(NASTY, fresh.run("j1@1000").orElseThrow().detail());
        assertEquals("j1", fresh.jobIdForKey("k1\tx").orElseThrow());
        assertEquals(RunState.SCHEDULED, fresh.run("j4@4000").orElseThrow().state());
        assertEquals(null, fresh.run("j4@4000").orElseThrow().workerId());
        assertEquals(RunState.ASSIGNED, fresh.run("j2@2000").orElseThrow().state());
    }

    @Test
    void aRestoredMachineBehavesLikeTheOriginalOnLaterCommands() {
        populate();
        SchedulerStateMachine copy = new SchedulerStateMachine(
                SchedulerStateMachine.DEFAULT_IDEMPOTENCY_RETENTION_MILLIS,
                (i, c, o) -> outcomesB.add(i + ":" + c + "=>" + o));
        copy.restore(sm.snapshot());
        outcomesA.clear();
        List<Command> more = List.of(
                new Command.AssignRun("j4@4000", "w3", 30),
                new Command.RunStarted("j2@2000", 9999),
                new Command.RunCompleted("j1@1000", 6, true, "late"),
                new Command.SubmitJob("j1", JobSpec.once("effect", "5", 1000, 3), "other", 500),
                new Command.SubmitJob("j9", JobSpec.once("effect", "5", 9000, 3), "k4", 501),
                new Command.SubmitJob("j8", JobSpec.once("effect", "5", 8000, 3), "k8", 502),
                new Command.FireRun("j8", 8000, 0));
        for (Command c : more) {
            long i = ++index;
            sm.apply(i, entry(c));
            copy.apply(i, entry(c));
        }
        assertEquals(outcomesA, outcomesB);
        assertEquals(sm.digest(), copy.digest());
        assertTrue(outcomesA.size() == more.size());
    }

    @Test
    void aBadSnapshotIsRejectedAndLeavesTheStateUntouched() {
        populate();
        String before = sm.digest();
        assertThrows(IllegalArgumentException.class, () -> sm.restore("V2\n"));
        assertThrows(IllegalArgumentException.class, () -> sm.restore("V1\nX\tjunk\n"));
        assertEquals(before, sm.digest());
        sm.restore("V1\n");
        assertEquals("", sm.digest());
    }
}
