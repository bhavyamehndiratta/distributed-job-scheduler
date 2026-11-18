package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Runs seeded chaos scenarios. By default 300 seeds, which is quick enough for
 * every build. For a bigger sweep:
 * {@code mvn -B test -Dtest=ChaosTest -Dchaos.seeds=2000 -Dchaos.start=1}.
 */
class ChaosTest {

    @Test
    void manySeededScenariosKeepEverySafetyProperty() {
        long start = Long.getLong("chaos.start", 1L);
        int count = Integer.getInteger("chaos.seeds", 300);
        List<String> failures = new ArrayList<>();
        long proposals = 0;
        long committed = 0;
        long crashes = 0;
        long partitions = 0;
        long terms = 0;
        long snapshots = 0;
        long installs = 0;
        for (long seed = start; seed < start + count; seed++) {
            ChaosHarness.Result r = ChaosHarness.run(seed);
            if (!r.ok()) {
                failures.add("seed " + seed + " (" + r.clusterSize() + " nodes): " + r.violations());
            }
            proposals += r.proposals();
            committed += r.committedCommands();
            crashes += r.crashes();
            partitions += r.partitions();
            terms += r.electedTerms();
            snapshots += r.snapshotsTaken();
            installs += r.snapshotInstalls();
        }
        System.out.println("CHAOS SUMMARY: " + count + " scenarios from seed " + start + ", " + failures.size()
                + " failed; " + proposals + " proposals, " + committed + " committed commands, " + crashes
                + " crashes, " + partitions + " partitions, " + terms + " elected terms, " + snapshots
                + " snapshots taken, " + installs + " snapshots installed");
        if (count >= 100) {
            assertTrue(snapshots > 0 && installs > 0, "snapshots " + snapshots + ", installs " + installs);
        }
        assertTrue(failures.isEmpty(), failures.size() + " scenario(s) failed:\n" + String.join("\n", failures));
    }

    @Test
    void aSeedReplaysTheExactSameRun() {
        assertEquals(ChaosHarness.run(4242), ChaosHarness.run(4242));
        assertEquals(ChaosHarness.run(7), ChaosHarness.run(7));
    }

    @Test
    void scenariosActuallyExerciseFailures() {
        long crashes = 0;
        long partitions = 0;
        long committed = 0;
        for (long seed = 1; seed <= 50; seed++) {
            ChaosHarness.Result r = ChaosHarness.run(seed);
            crashes += r.crashes();
            partitions += r.partitions();
            committed += r.committedCommands();
        }
        assertTrue(crashes > 100, "crashes: " + crashes);
        assertTrue(partitions > 100, "partitions: " + partitions);
        assertTrue(committed > 500, "committed commands: " + committed);
    }

    // ---- the checker must be able to fail ----

    private static TestCluster clusterWithCommittedEntries() {
        TestCluster c = new TestCluster(3, 99);
        c.awaitSingleLeader(200);
        for (int i = 0; i < 5; i++) {
            c.propose("c" + i);
            c.runFor(10);
        }
        c.runFor(50);
        assertTrue(ChaosHarness.checkFinal(c).isEmpty(), "a healthy cluster must pass the final check");
        return c;
    }

    @Test
    void checkerFlagsALostAcknowledgedEntry() {
        TestCluster c = clusterWithCommittedEntries();
        RaftNode leader = c.leaders().get(0);
        for (var id : c.members()) {
            if (!id.equals(leader.id())) {
                c.log(id).truncateFrom(3);
                break;
            }
        }
        List<String> bad = ChaosHarness.checkFinal(c);
        assertFalse(bad.isEmpty());
        assertTrue(bad.stream().anyMatch(s -> s.contains("lost acknowledged entry")), bad.toString());
    }

    @Test
    void checkerFlagsALeaderMissingACommittedEntry() {
        TestCluster c = clusterWithCommittedEntries();
        c.log(c.leaders().get(0).id()).truncateFrom(3);
        List<String> bad = ChaosHarness.checkContinuously(c, 1);
        assertTrue(bad.stream().anyMatch(s -> s.contains("leader completeness")), bad.toString());
    }

    @Test
    void checkerFlagsDivergentLogs() {
        TestCluster c = clusterWithCommittedEntries();
        RaftNode leader = c.leaders().get(0);
        for (var id : c.members()) {
            if (!id.equals(leader.id())) {
                long last = c.log(id).lastIndex();
                long term = c.log(id).termAt(last);
                c.log(id).truncateFrom(last);
                c.log(id).append(new LogEntry(term, "forged"));
                break;
            }
        }
        List<String> bad = ChaosHarness.checkFinal(c);
        assertFalse(bad.isEmpty());
    }
}
