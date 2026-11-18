package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SchedulerSnapshotChaosTest {

    @Test
    void theSchedulerSurvivesChaosWithSnapshotsAndInstalls() {
        int count = Integer.getInteger("snapshotChaos.scenarios", 100);
        long snapshots = 0;
        long installs = 0;
        long succeeded = 0;
        int failed = 0;
        StringBuilder bad = new StringBuilder();
        for (long seed = 1; seed <= count; seed++) {
            SchedulerChaosHarness.Result r = SchedulerChaosHarness.run(seed);
            snapshots += r.snapshotsTaken();
            installs += r.snapshotInstalls();
            succeeded += r.succeeded();
            if (!r.ok()) {
                failed++;
                bad.append("seed ").append(seed).append(": ").append(r.violations()).append('\n');
            }
        }
        System.out.println("SCHED SNAPSHOT CHAOS SUMMARY: " + count + " scenarios from seed 1, " + failed
                + " failed; " + succeeded + " jobs succeeded; " + snapshots + " snapshots taken, " + installs
                + " snapshots installed");
        assertTrue(failed == 0, bad.toString());
        assertTrue(snapshots > 0 && installs > 0, "snapshots " + snapshots + ", installs " + installs);
    }
}
