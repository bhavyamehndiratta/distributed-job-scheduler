package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

class MembershipTest {

    private static NodeId n(int i) {
        return new NodeId("n" + i);
    }

    private static Set<NodeId> set(int... ids) {
        Set<NodeId> out = new TreeSet<>();
        for (int i : ids) {
            out.add(n(i));
        }
        return out;
    }

    private static void load(TestCluster c, int count) {
        for (int i = 0; i < count; i++) {
            c.propose("c" + i);
            c.runFor(3);
        }
    }

    private static void assertSafe(TestCluster c) {
        assertTrue(c.electionSafetyViolations().isEmpty(), c.electionSafetyViolations().toString());
        assertTrue(c.stateMachineViolations().isEmpty(), c.stateMachineViolations().toString());
        assertTrue(c.logMatchingViolations().isEmpty(), c.logMatchingViolations().toString());
    }

    @Test
    void configurationsRoundTripAndRejectNonsense() {
        ClusterConfig c = new ClusterConfig(set(1, 2, 3), set(4));
        assertEquals(c, ClusterConfig.decode(c.encode()));
        assertEquals(ClusterConfig.EMPTY, ClusterConfig.decode(ClusterConfig.EMPTY.encode()));
        assertTrue(ClusterConfig.isConfigCommand(c.encode()));
        assertFalse(ClusterConfig.isConfigCommand("SubmitJob|x"));
        assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(set(1), set(1)));
        assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(Set.of(new NodeId("a|b")), Set.of()));
        assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(Set.of(new NodeId("a,b")), Set.of()));
    }

    @Test
    void clientCommandsCannotImpersonateAConfigurationEntry() {
        TestCluster c = new TestCluster(3, 40);
        RaftNode leader = c.awaitSingleLeader(300);
        String forged = new ClusterConfig(set(1), Set.of()).encode();
        assertThrows(IllegalArgumentException.class, () -> leader.propose(forged));
    }

    @Test
    void aLearnerCatchesUpWithoutVotingAndThenIsPromoted() {
        TestCluster c = new TestCluster(3, 41);
        RaftNode leader = c.awaitSingleLeader(300);
        c.runFor(30);
        load(c, 20);
        c.addNode(n(4));
        assertTrue(leader.addLearner(n(4)) > 0);
        c.runFor(100);
        RaftNode learner = c.node(n(4));
        assertEquals(set(4), leader.learners());
        assertEquals(set(4), learner.learners());
        assertEquals(set(1, 2, 3), learner.voters());
        assertEquals(leader.commitIndex(), learner.commitIndex());
        assertEquals(Role.FOLLOWER, learner.role());
        assertEquals(leader.currentTerm(), learner.currentTerm());

        assertTrue(leader.promote(n(4)) > 0);
        c.runFor(100);
        for (NodeId id : c.members()) {
            assertEquals(set(1, 2, 3, 4), c.node(id).voters(), id.toString());
            assertEquals(Set.of(), c.node(id).learners(), id.toString());
        }
        load(c, 10);
        c.runFor(100);
        String state = c.stateOf(leader.id());
        for (NodeId id : c.members()) {
            assertEquals(state, c.stateOf(id), id.toString());
        }
        assertSafe(c);
    }

    @Test
    void onlyOneChangeAtATimeAndOnlyACaughtUpLearnerIsPromoted() {
        TestCluster c = new TestCluster(3, 42);
        RaftNode leader = c.awaitSingleLeader(300);
        c.runFor(30);
        c.addNode(n(4));
        c.addNode(n(5));
        assertTrue(leader.addLearner(n(4)) > 0);
        assertEquals(-1, leader.addLearner(n(5)), "the first change has not committed yet");
        c.runFor(30);
        c.crash(n(4));
        load(c, 120);
        assertTrue(leader.addLearner(n(5)) > 0, "the first change committed, so the next may start");
        c.runFor(60);
        assertEquals(-1, leader.promote(n(4)), "n4 is far behind");
        assertEquals(-1, leader.promote(n(1)), "a voter is not a learner");
        assertEquals(-1, leader.addLearner(n(4)), "already a learner");
        assertEquals(set(4, 5), leader.learners());
        assertSafe(c);
    }

    @Test
    void aFollowerCanBeRemovedAndNeverDisturbsTheCluster() {
        TestCluster c = new TestCluster(3, 43);
        RaftNode leader = c.awaitSingleLeader(300);
        c.runFor(30);
        NodeId gone = null;
        for (NodeId id : c.members()) {
            if (!id.equals(leader.id())) {
                gone = id;
                break;
            }
        }
        long term = leader.currentTerm();
        int terms = c.electedTerms();
        assertTrue(leader.remove(gone) > 0);
        c.runFor(60);
        assertEquals(2, leader.voters().size());
        assertFalse(leader.isVoter(gone));
        load(c, 20);
        c.runFor(400);
        assertEquals(Role.LEADER, leader.role());
        assertEquals(term, leader.currentTerm());
        assertEquals(terms, c.electedTerms());
        assertSafe(c);
    }

    @Test
    void aLeaderThatRemovesItselfStepsDownAndTheRestCarryOn() {
        TestCluster c = new TestCluster(3, 44);
        RaftNode old = c.awaitSingleLeader(300);
        c.runFor(30);
        assertTrue(old.remove(old.id()) > 0);
        c.runFor(300);
        assertEquals(Role.FOLLOWER, old.role());
        List<RaftNode> leaders = c.leaders();
        assertEquals(1, leaders.size());
        RaftNode neu = leaders.get(0);
        assertNotEquals(old.id(), neu.id());
        assertFalse(neu.isVoter(old.id()));
        assertEquals(2, neu.voters().size());
        long index = neu.propose("after");
        c.runFor(50);
        assertTrue(neu.commitIndex() >= index);
        c.runFor(300);
        assertEquals(1, c.leaders().size());
        assertEquals(neu.id(), c.leaders().get(0).id());
        assertSafe(c);
    }

    @Test
    void anUncommittedConfigurationEntryIsRevertedWhenItIsOverwritten() {
        TestCluster c = new TestCluster(3, 45);
        RaftNode old = c.awaitSingleLeader(300);
        c.runFor(30);
        c.addNode(n(4));
        Set<NodeId> others = new TreeSet<>();
        for (NodeId id : c.members()) {
            if (!id.equals(old.id())) {
                others.add(id);
            }
        }
        c.net.partition(Set.of(old.id()), others);
        assertTrue(old.addLearner(n(4)) > 0);
        assertEquals(set(4), old.learners(), "a node uses a configuration as soon as it is in its log");
        c.runFor(150);
        RaftNode neu = null;
        for (RaftNode l : c.leaders()) {
            if (!l.id().equals(old.id())) {
                neu = l;
            }
        }
        assertTrue(neu != null, "the majority side elects a leader");
        for (int i = 0; i < 5; i++) {
            neu.propose("x" + i);
            c.runFor(5);
        }
        c.net.heal();
        c.runFor(300);
        assertEquals(Set.of(), old.learners(), "the uncommitted change was overwritten");
        assertEquals(set(1, 2, 3), old.voters());
        assertEquals(old.commitIndex(), c.node(neu.id()).commitIndex());
        assertSafe(c);
    }

    @Test
    void aRemovedNodeThatNeverHeardAboutItCannotWinAnElection() {
        TestCluster c = new TestCluster(5, 47);
        RaftNode leader = c.awaitSingleLeader(300);
        c.runFor(30);
        NodeId gone = null;
        for (NodeId id : c.members()) {
            if (!id.equals(leader.id())) {
                gone = id;
                break;
            }
        }
        assertTrue(leader.remove(gone) > 0);
        c.runFor(60);
        c.crash(leader.id());
        c.runFor(500);
        List<RaftNode> leaders = c.leaders();
        assertEquals(1, leaders.size());
        assertNotEquals(gone, leaders.get(0).id());
        assertNotEquals(leader.id(), leaders.get(0).id());
        assertFalse(leaders.get(0).isVoter(gone));
        assertNotEquals(Role.LEADER, c.node(gone).role());
        assertSafe(c);
    }

    @Test
    void aLearnerThatNeverReceivedItsOwnAddEntryIsNotPromoted() {
        TestCluster c = new TestCluster(3, 49);
        RaftNode leader = c.awaitSingleLeader(300);
        c.runFor(30);
        c.addNode(n(4));
        c.crash(n(4));
        assertTrue(leader.addLearner(n(4)) > 0);
        c.runFor(40);
        assertEquals(-1, leader.promote(n(4)), "a node that knows no configuration must not become a voter");
        assertSafe(c);
    }

    @Test
    void addressesTravelWithTheConfigurationAndFollowTheNode() {
        ClusterConfig base = new ClusterConfig(set(1, 2, 3), Set.of());
        ClusterConfig added = base.withLearner(n(4), "127.0.0.1:7004");
        assertEquals("127.0.0.1:7004", added.addresses().get(n(4)));
        assertEquals(added, ClusterConfig.decode(added.encode()));
        assertEquals("127.0.0.1:7004", added.promoted(n(4)).addresses().get(n(4)));
        assertTrue(added.without(n(4)).addresses().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> base.withLearner(n(5), "bad|address"));
        assertThrows(IllegalArgumentException.class, () -> base.withLearner(n(5), "a,b"));
    }

    @Test
    void theConfigurationSurvivesSnapshotsAndRestarts() {
        TestCluster c = new TestCluster(3, 48, 10);
        RaftNode leader = c.awaitSingleLeader(300);
        c.runFor(30);
        load(c, 40);
        c.addNode(n(4));
        assertTrue(leader.addLearner(n(4)) > 0);
        c.runFor(150);
        assertTrue(c.node(n(4)).snapshotsInstalled() >= 1, "n4 joins a log that was already compacted");
        assertTrue(leader.promote(n(4)) > 0);
        c.runFor(100);
        load(c, 60);
        c.runFor(100);
        assertTrue(c.node(n(4)).snapshotIndex() > 0);
        c.crash(n(4));
        c.restart(n(4));
        assertEquals(set(1, 2, 3, 4), c.node(n(4)).voters(), "restored from its own snapshot and log");
        c.crash(leader.id());
        c.restart(leader.id());
        assertEquals(set(1, 2, 3, 4), c.node(leader.id()).voters());
        c.runFor(300);
        String state = null;
        for (NodeId id : c.members()) {
            state = state == null ? c.stateOf(id) : state;
            assertEquals(state, c.stateOf(id), id.toString());
        }
        assertSafe(c);
    }
}
