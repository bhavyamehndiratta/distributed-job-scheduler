package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.net.Transport;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SnapshotTest {

    private static final NodeId N1 = new NodeId("n1");

    @Test
    void theLogStaysShortBecauseNodesSnapshot() {
        TestCluster c = new TestCluster(3, 31, 10);
        RaftNode leader = c.awaitSingleLeader(300);
        for (int i = 0; i < 80; i++) {
            c.propose("c" + i);
            c.runFor(3);
        }
        c.runFor(100);
        for (NodeId id : c.members()) {
            RaftNode n = c.node(id);
            assertTrue(n.snapshotIndex() > 0, id + " never snapshotted");
            assertTrue(n.lastLogIndex() - n.snapshotIndex() <= 15, id + " keeps " + (n.lastLogIndex() - n.snapshotIndex()));
        }
        assertTrue(leader.snapshotsTaken() > 3);
        assertTrue(ChaosHarness.checkFinal(c).isEmpty(), ChaosHarness.checkFinal(c).toString());
    }

    @Test
    void aFollowerThatFellFarBehindCatchesUpThroughInstallSnapshot() {
        TestCluster c = new TestCluster(3, 32, 10);
        RaftNode leader = c.awaitSingleLeader(300);
        NodeId lagging = null;
        NodeId other = null;
        for (NodeId id : c.members()) {
            if (id.equals(leader.id())) {
                continue;
            }
            if (lagging == null) {
                lagging = id;
            } else {
                other = id;
            }
        }
        c.net.partition(Set.of(leader.id(), other), Set.of(lagging));
        for (int i = 0; i < 80; i++) {
            c.propose("c" + i);
            c.runFor(3);
        }
        c.runFor(50);
        assertTrue(leader.snapshotIndex() > 20, "leader must have compacted past what the follower has");
        c.net.heal();
        c.runFor(300);
        assertTrue(c.node(lagging).snapshotsInstalled() >= 1, "the follower should have installed a snapshot");
        assertEquals(c.node(leader.id()).commitIndex(), c.node(lagging).commitIndex());
        assertEquals(c.stateOf(leader.id()), c.stateOf(lagging));
        assertTrue(ChaosHarness.checkFinal(c).isEmpty(), ChaosHarness.checkFinal(c).toString());
    }

    @Test
    void aRestartedNodeStartsFromItsSnapshotAndCatchesUp() {
        TestCluster c = new TestCluster(3, 33, 10);
        RaftNode leader = c.awaitSingleLeader(300);
        NodeId victim = null;
        for (NodeId id : c.members()) {
            if (!id.equals(leader.id())) {
                victim = id;
                break;
            }
        }
        for (int i = 0; i < 60; i++) {
            c.propose("c" + i);
            c.runFor(3);
        }
        c.runFor(50);
        c.crash(victim);
        for (int i = 60; i < 80; i++) {
            c.propose("c" + i);
            c.runFor(3);
        }
        c.restart(victim);
        RaftNode restarted = c.node(victim);
        assertTrue(restarted.snapshotIndex() > 0);
        assertEquals(restarted.snapshotIndex(), restarted.lastApplied(), "state is restored at construction");
        assertEquals(restarted.snapshotIndex(), restarted.commitIndex());
        c.runFor(300);
        assertEquals(c.stateOf(c.leaders().get(0).id()), c.stateOf(victim));
        assertTrue(ChaosHarness.checkFinal(c).isEmpty(), ChaosHarness.checkFinal(c).toString());
    }

    @Test
    void aSingleNodeRestartsFromItsSnapshotFiles(@TempDir Path dir) {
        InMemoryPersistentState term = new InMemoryPersistentState();
        RaftConfig cfg = RaftConfig.defaults().withSnapshotThreshold(5);
        Transport none = (from, to, message) -> { };
        Map<Long, LogEntry> applied = new TreeMap<>();
        long firstSnapshot;
        try (FileRaftLog log = new FileRaftLog(dir.resolve("log"))) {
            SnapshotRecorder rec = new SnapshotRecorder(applied::putIfAbsent);
            RaftNode node = new RaftNode(N1, List.of(N1), none, new Random(1), cfg, term, log, rec,
                    new FileSnapshotStore(dir.resolve("snap")), RaftObserver.NONE);
            for (int i = 0; i < 40; i++) {
                node.tick();
            }
            assertEquals(Role.LEADER, node.role());
            for (int i = 0; i < 23; i++) {
                assertTrue(node.propose("c" + i) > 0);
            }
            assertEquals(24, node.lastApplied());
            firstSnapshot = node.snapshotIndex();
            assertTrue(firstSnapshot >= 20, "snapshot index " + firstSnapshot);
        }
        try (FileRaftLog log = new FileRaftLog(dir.resolve("log"))) {
            SnapshotRecorder rec = new SnapshotRecorder(applied::putIfAbsent);
            RaftNode node = new RaftNode(N1, List.of(N1), none, new Random(2), cfg, term, log, rec,
                    new FileSnapshotStore(dir.resolve("snap")), RaftObserver.NONE);
            assertEquals(firstSnapshot, node.snapshotIndex());
            assertEquals(firstSnapshot, node.lastApplied(), "restored from the snapshot, nothing replayed yet");
            for (int i = 0; i < 60; i++) {
                node.tick();
            }
            assertEquals(Role.LEADER, node.role());
            assertEquals(25, node.lastApplied());
            assertEquals(SnapshotRecorder.expected(applied, 25), rec.state());
        }
    }

    @Test
    void aCrashBetweenSavingTheSnapshotAndCompactingTheLogIsFinishedOnStart(@TempDir Path dir) {
        InMemoryPersistentState term = new InMemoryPersistentState();
        RaftConfig cfg = RaftConfig.defaults();
        Transport none = (from, to, message) -> { };
        Map<Long, LogEntry> applied = new TreeMap<>();
        Snapshot midway;
        try (FileRaftLog log = new FileRaftLog(dir.resolve("log"))) {
            SnapshotRecorder rec = new SnapshotRecorder(applied::putIfAbsent);
            RaftNode node = new RaftNode(N1, List.of(N1), none, new Random(1), cfg, term, log, rec,
                    new FileSnapshotStore(dir.resolve("snap")), RaftObserver.NONE);
            for (int i = 0; i < 40; i++) {
                node.tick();
            }
            for (int i = 0; i < 20; i++) {
                node.propose("c" + i);
            }
            assertEquals(21, node.lastApplied());
            SnapshotRecorder replay = new SnapshotRecorder((i, e) -> { });
            for (long i = 1; i <= 15; i++) {
                replay.apply(i, log.get(i));
            }
            midway = new Snapshot(15, log.termAt(15),
                    RaftNode.withConfig(ClusterConfig.of(List.of(N1)), replay.snapshot()));
        }
        // The snapshot reached disk, the log still has everything: the state a crash in between leaves behind.
        new FileSnapshotStore(dir.resolve("snap")).save(midway);
        try (FileRaftLog log = new FileRaftLog(dir.resolve("log"))) {
            SnapshotRecorder rec = new SnapshotRecorder(applied::putIfAbsent);
            RaftNode node = new RaftNode(N1, List.of(N1), none, new Random(2), cfg, term, log, rec,
                    new FileSnapshotStore(dir.resolve("snap")), RaftObserver.NONE);
            assertEquals(15, node.snapshotIndex());
            assertEquals(21, log.lastIndex(), "entries after the snapshot are kept");
            assertEquals(15, node.lastApplied());
            for (int i = 0; i < 60; i++) {
                node.tick();
            }
            assertEquals(22, node.lastApplied());
            assertEquals(SnapshotRecorder.expected(applied, 22), rec.state());
        }
    }
}
