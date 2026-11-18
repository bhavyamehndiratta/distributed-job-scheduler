package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Seeded scheduler chaos. Default 200 seeds. For a bigger sweep:
 * {@code mvn -B test -Dtest=SchedulerChaosTest -Dschedchaos.seeds=1000 -Dschedchaos.start=1}.
 */
class SchedulerChaosTest {

    @Test
    void manySeededScenariosKeepEverySchedulerProperty() {
        long start = Long.getLong("schedchaos.start", 1L);
        int count = Integer.getInteger("schedchaos.seeds", 200);
        List<String> failures = new ArrayList<>();
        long submitted = 0;
        long acked = 0;
        long succeeded = 0;
        long nodeCrashes = 0;
        long workerCrashes = 0;
        long pauses = 0;
        long partitions = 0;
        long readsSent = 0;
        long readsAnswered = 0;
        for (long seed = start; seed < start + count; seed++) {
            SchedulerChaosHarness.Result r = SchedulerChaosHarness.run(seed);
            if (!r.ok()) {
                failures.add("seed " + seed + " (" + r.nodes() + " nodes): " + r.violations());
            }
            submitted += r.submitted();
            acked += r.acked();
            succeeded += r.succeeded();
            nodeCrashes += r.nodeCrashes();
            workerCrashes += r.workerCrashes();
            pauses += r.pauses();
            partitions += r.partitions();
            readsSent += r.readsSent();
            readsAnswered += r.readsAnswered();
        }
        System.out.println("SCHED CHAOS SUMMARY: " + count + " scenarios from seed " + start + ", " + failures.size()
                + " failed; " + submitted + " jobs submitted, " + acked + " acked, " + succeeded
                + " succeeded with exactly one effect; " + nodeCrashes + " node crashes, " + workerCrashes
                + " worker crashes, " + pauses + " worker pauses, " + partitions + " partitions; " + readsSent
                + " read requests, " + readsAnswered + " answered");
        assertTrue(failures.isEmpty(), failures.subList(0, Math.min(5, failures.size())).toString());
        assertTrue(acked > 0 && succeeded == acked, "acked " + acked + " succeeded " + succeeded);
        assertTrue(nodeCrashes > 0 && workerCrashes > 0 && pauses > 0 && partitions > 0, "faults were not injected");
        assertTrue(readsAnswered > 0, "no read was ever answered");
    }

    @Test
    void anUnprotectedEffectStoreIsCaughtDoingDuplicateEffects() {
        int caught = 0;
        for (long seed = 1; seed <= 60; seed++) {
            SchedulerChaosHarness.Result r = SchedulerChaosHarness.run(seed, false);
            if (r.violations().stream().anyMatch(v -> v.contains("duplicate effect"))) {
                caught++;
            }
        }
        System.out.println("SCHED CHAOS NAIVE STORE: duplicates caught in " + caught + " of 60 seeds");
        assertTrue(caught > 0, "the checker never saw a duplicate on a store with no protection");
    }

    @Test
    void theProtectedStoreHasNoDuplicatesOnTheSameSeeds() {
        for (long seed = 1; seed <= 60; seed++) {
            SchedulerChaosHarness.Result r = SchedulerChaosHarness.run(seed, true);
            assertEquals(0, r.duplicateEffects(), "seed " + seed);
        }
    }

    @Test
    void aSeedReplaysIdentically() {
        assertEquals(SchedulerChaosHarness.run(42), SchedulerChaosHarness.run(42));
        assertEquals(SchedulerChaosHarness.run(7), SchedulerChaosHarness.run(7));
    }
}
