package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * Seeded chaos with membership changes. Default 200 seeds; for a bigger sweep:
 * {@code mvn -B test -Dtest=MembershipChaosTest -Dmembership.seeds=2000 -Dmembership.start=1}.
 */
class MembershipChaosTest {

    @Test
    void manySeededScenariosKeepEverySafetyPropertyWhileTheClusterChangesShape() {
        long start = Long.getLong("membership.start", 1L);
        int count = Integer.getInteger("membership.seeds", 200);
        List<String> failures = new ArrayList<>();
        long proposals = 0;
        long committed = 0;
        long crashes = 0;
        long partitions = 0;
        long added = 0;
        long promoted = 0;
        long removed = 0;
        long configs = 0;
        long snapshots = 0;
        long installs = 0;
        for (long seed = start; seed < start + count; seed++) {
            MembershipChaosHarness.Result r = MembershipChaosHarness.run(seed);
            if (!r.ok()) {
                failures.add("seed " + seed + ": " + r.violations());
            }
            proposals += r.proposals();
            committed += r.committedCommands();
            crashes += r.crashes();
            partitions += r.partitions();
            added += r.learnersAdded();
            promoted += r.promotions();
            removed += r.removals();
            configs += r.committedConfigs();
            snapshots += r.snapshotsTaken();
            installs += r.snapshotInstalls();
        }
        System.out.println("MEMBERSHIP CHAOS SUMMARY: " + count + " scenarios from seed " + start + ", "
                + failures.size() + " failed; " + proposals + " proposals, " + committed
                + " committed commands, " + crashes + " crashes, " + partitions + " partitions; "
                + added + " learners added, " + promoted + " promotions, " + removed + " removals, "
                + configs + " configuration entries committed; " + snapshots + " snapshots taken, "
                + installs + " installed");
        if (count >= 100) {
            assertTrue(added > 0 && promoted > 0 && removed > 0 && configs > 0,
                    "added " + added + ", promoted " + promoted + ", removed " + removed + ", configs " + configs);
            assertTrue(snapshots > 0 && installs > 0, "snapshots " + snapshots + ", installs " + installs);
        }
        assertTrue(failures.isEmpty(), failures.size() + " scenario(s) failed:\n" + String.join("\n", failures));
    }

    @Test
    void aSeedReplaysTheExactSameRun() {
        assertEquals(MembershipChaosHarness.run(4242), MembershipChaosHarness.run(4242));
        assertEquals(MembershipChaosHarness.run(7), MembershipChaosHarness.run(7));
    }

    // ---- the checker must be able to fail ----

    private static Set<NodeId> ids(int... values) {
        Set<NodeId> out = new TreeSet<>();
        for (int v : values) {
            out.add(new NodeId("n" + v));
        }
        return out;
    }

    private static LogEntry cfg(Set<NodeId> voters, Set<NodeId> learners) {
        return new LogEntry(1, new ClusterConfig(voters, learners).encode());
    }

    @Test
    void chainCheckFlagsAConfigurationThatChangesTwoVotersAtOnce() {
        ClusterConfig initial = new ClusterConfig(ids(1, 2, 3), Set.of());
        Map<Long, LogEntry> ok = new TreeMap<>();
        ok.put(2L, cfg(ids(1, 2, 3), ids(4)));
        ok.put(5L, cfg(ids(1, 2, 3, 4), Set.of()));
        ok.put(9L, cfg(ids(1, 2, 4), Set.of()));
        assertTrue(MembershipChaosHarness.configChainViolations(ok, initial).isEmpty());

        Map<Long, LogEntry> jump = new TreeMap<>();
        jump.put(2L, cfg(ids(1, 2, 3, 4, 5), Set.of()));
        assertFalse(MembershipChaosHarness.configChainViolations(jump, initial).isEmpty());

        Map<Long, LogEntry> swap = new TreeMap<>();
        swap.put(2L, cfg(ids(1, 2, 4), Set.of()));
        assertFalse(MembershipChaosHarness.configChainViolations(swap, initial).isEmpty());
    }

    @Test
    void finalCheckFlagsALostAcknowledgedEntryOnAMember() {
        TestCluster c = new TestCluster(3, 99);
        c.awaitSingleLeader(200);
        for (int i = 0; i < 5; i++) {
            c.propose("c" + i);
            c.runFor(10);
        }
        c.runFor(50);
        ClusterConfig initial = ClusterConfig.of(c.members());
        Set<NodeId> members = new TreeSet<>(c.members());
        assertTrue(MembershipChaosHarness.checkFinal(c, members, initial).isEmpty());
        RaftNode leader = c.leaders().get(0);
        for (NodeId id : c.members()) {
            if (!id.equals(leader.id())) {
                c.log(id).truncateFrom(3);
                break;
            }
        }
        List<String> bad = MembershipChaosHarness.checkFinal(c, members, initial);
        assertTrue(bad.stream().anyMatch(s -> s.contains("lost acknowledged entry")), bad.toString());
    }
}
