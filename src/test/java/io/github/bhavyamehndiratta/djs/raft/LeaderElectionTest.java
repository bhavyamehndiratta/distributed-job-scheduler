package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LeaderElectionTest {

    @Test
    void threeNodesElectExactlyOneLeaderThatEveryoneFollows() {
        TestCluster c = new TestCluster(3, 1);
        RaftNode leader = c.awaitSingleLeader(200);
        c.runFor(50);
        assertEquals(1, c.leaders().size());
        for (NodeId id : c.members()) {
            assertEquals(leader.id(), c.node(id).leaderId(), id + " should follow the leader");
            assertEquals(leader.currentTerm(), c.node(id).currentTerm());
        }
        assertTrue(c.electionSafetyViolations().isEmpty());
    }

    @Test
    void fiveNodesElectALeader() {
        TestCluster c = new TestCluster(5, 2);
        c.awaitSingleLeader(200);
        c.runFor(100);
        assertEquals(1, c.leaders().size());
        assertTrue(c.electionSafetyViolations().isEmpty());
    }

    @Test
    void stableLeaderKeepsItsTermWhenNothingFails() {
        TestCluster c = new TestCluster(3, 3);
        RaftNode leader = c.awaitSingleLeader(200);
        long term = leader.currentTerm();
        c.runFor(1_000);
        assertEquals(Role.LEADER, leader.role());
        assertEquals(term, leader.currentTerm(), "heartbeats must prevent new elections");
    }

    @Test
    void newLeaderIsElectedAfterTheLeaderCrashes() {
        TestCluster c = new TestCluster(3, 4);
        RaftNode old = c.awaitSingleLeader(200);
        c.crash(old.id());
        RaftNode replacement = c.awaitSingleLeader(200);
        assertNotEquals(old.id(), replacement.id());
        assertTrue(replacement.currentTerm() > old.currentTerm());
        assertTrue(c.electionSafetyViolations().isEmpty());
    }

    @Test
    void restartedOldLeaderRejoinsAsFollower() {
        TestCluster c = new TestCluster(3, 5);
        NodeId oldId = c.awaitSingleLeader(200).id();
        c.crash(oldId);
        RaftNode replacement = c.awaitSingleLeader(200);
        c.restart(oldId);
        c.runFor(100);
        RaftNode restarted = c.node(oldId);
        assertEquals(Role.FOLLOWER, restarted.role());
        assertEquals(replacement.id(), restarted.leaderId());
        assertEquals(replacement.currentTerm(), restarted.currentTerm());
    }

    @Test
    void clusterWithTwoOfThreeNodesDownCannotElect() {
        TestCluster c = new TestCluster(3, 6);
        RaftNode leader = c.awaitSingleLeader(200);
        for (NodeId id : c.members()) {
            if (!id.equals(leader.id())) {
                c.crash(id);
            }
        }
        c.runFor(300);
        assertTrue(c.leaders().isEmpty(), "a lone node must not stay or become leader");
    }

    @Test
    void checkQuorumMakesAnIsolatedLeaderStepDown() {
        TestCluster c = new TestCluster(3, 7);
        RaftNode leader = c.awaitSingleLeader(200);
        Set<NodeId> rest = new HashSet<>(c.members());
        rest.remove(leader.id());
        c.net.partition(Set.of(leader.id()), rest);
        c.runFor(3 * RaftConfig.defaults().electionTimeoutTicks());
        assertNotEquals(Role.LEADER, leader.role());
    }

    @Test
    void majoritySideElectsANewLeaderDuringAPartition() {
        TestCluster c = new TestCluster(5, 8);
        RaftNode old = c.awaitSingleLeader(200);
        List<NodeId> others = new ArrayList<>(c.members());
        others.remove(old.id());
        Set<NodeId> minority = Set.of(old.id(), others.get(0));
        Set<NodeId> majority = Set.copyOf(others.subList(1, others.size()));
        c.net.partition(minority, majority);
        c.runFor(200);
        long leadersInMajority = c.leaders().stream().filter(n -> majority.contains(n.id())).count();
        assertEquals(1, leadersInMajority);
        assertFalse(c.leaders().stream().anyMatch(n -> minority.contains(n.id())));
        assertTrue(c.electionSafetyViolations().isEmpty());
    }

    @Test
    void preVoteStopsAnIsolatedFollowerFromInflatingItsTerm() {
        TestCluster c = new TestCluster(3, 9);
        RaftNode leader = c.awaitSingleLeader(200);
        NodeId follower = c.members().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        long termBefore = c.node(follower).currentTerm();
        Set<NodeId> rest = new HashSet<>(c.members());
        rest.remove(follower);
        c.net.partition(Set.of(follower), rest);
        c.runFor(500);
        assertEquals(termBefore, c.node(follower).currentTerm(),
                "without PreVote the isolated node would bump its term every timeout");
        c.net.heal();
        c.runFor(100);
        assertEquals(Role.LEADER, leader.role(), "rejoining node must not disrupt the healthy leader");
        assertEquals(termBefore, leader.currentTerm());
    }

    @Test
    void nodeGrantsAtMostOneVotePerTerm() {
        List<Message> sent = new ArrayList<>();
        NodeId a = new NodeId("a");
        NodeId b = new NodeId("b");
        NodeId c = new NodeId("c");
        RaftNode node = new RaftNode(a, List.of(a, b, c), (from, to, msg) -> sent.add(msg),
                new Random(1), RaftConfig.defaults(), new InMemoryPersistentState(), RaftObserver.NONE);
        node.handle(b, new RequestVote(1, b, 0, 0, false));
        node.handle(c, new RequestVote(1, c, 0, 0, false));
        node.handle(b, new RequestVote(1, b, 0, 0, false));
        assertEquals(new RequestVoteResponse(1, true, false), sent.get(0));
        assertEquals(new RequestVoteResponse(1, false, false), sent.get(1), "second candidate in same term");
        assertEquals(new RequestVoteResponse(1, true, false), sent.get(2), "repeat request from same candidate");
    }

    @Test
    void restartedNodeRemembersItsVote() {
        NodeId a = new NodeId("a");
        NodeId b = new NodeId("b");
        NodeId c = new NodeId("c");
        InMemoryPersistentState disk = new InMemoryPersistentState();
        List<Message> sent = new ArrayList<>();
        RaftNode before = new RaftNode(a, List.of(a, b, c), (from, to, msg) -> sent.add(msg),
                new Random(1), RaftConfig.defaults(), disk, RaftObserver.NONE);
        before.handle(b, new RequestVote(1, b, 0, 0, false));
        RaftNode after = new RaftNode(a, List.of(a, b, c), (from, to, msg) -> sent.add(msg),
                new Random(2), RaftConfig.defaults(), disk, RaftObserver.NONE);
        after.handle(c, new RequestVote(1, c, 0, 0, false));
        assertEquals(new RequestVoteResponse(1, false, false), sent.get(1), "must not vote twice in term 1 after restart");
    }

    @Test
    void staleLeaderStepsDownWhenToldAboutANewerTerm() {
        List<Message> sent = new ArrayList<>();
        NodeId a = new NodeId("a");
        NodeId b = new NodeId("b");
        RaftNode node = new RaftNode(a, List.of(a, b, new NodeId("c")), (from, to, msg) -> sent.add(msg),
                new Random(1), RaftConfig.defaults(), new InMemoryPersistentState(), RaftObserver.NONE);
        node.handle(b, new AppendEntries(5, b));
        assertEquals(5, node.currentTerm());
        node.handle(b, new AppendEntries(3, b));
        assertEquals(new AppendEntriesResponse(5, false), sent.get(sent.size() - 1));
    }

    @Test
    void nodeWithALiveLeaderRefusesPreVoteButGrantsItOnceTheLeaderIsSilent() {
        List<Message> sent = new ArrayList<>();
        NodeId a = new NodeId("a");
        NodeId b = new NodeId("b");
        NodeId c = new NodeId("c");
        RaftNode node = new RaftNode(a, List.of(a, b, c), (from, to, msg) -> sent.add(msg),
                new Random(1), RaftConfig.defaults(), new InMemoryPersistentState(), RaftObserver.NONE);
        node.handle(b, new AppendEntries(1, b));
        sent.clear();

        node.handle(c, new RequestVote(2, c, 0, 0, true));
        assertEquals(new RequestVoteResponse(1, false, true), sent.get(0), "leader b is alive, so refuse");
        assertEquals(1, node.currentTerm(), "PreVote must never change the term");

        for (int i = 0; i < 2 * RaftConfig.defaults().electionTimeoutTicks(); i++) {
            node.tick();
        }
        sent.clear();
        node.handle(c, new RequestVote(2, c, 0, 0, true));
        assertEquals(new RequestVoteResponse(2, true, true), sent.get(0), "leader silent, so grant");
    }

    @Test
    void randomizedFaultsNeverProduceTwoLeadersInOneTerm() {
        for (long seed = 1; seed <= 100; seed++) {
            TestCluster c = new TestCluster(5, seed);
            Random chaos = new Random(seed);
            c.net.setDropRate(0.1);
            c.net.setDuplicateRate(0.05);
            c.net.setDelayRange(1, 4);
            for (int round = 0; round < 20; round++) {
                List<NodeId> shuffled = new ArrayList<>(c.members());
                java.util.Collections.shuffle(shuffled, chaos);
                int cut = 1 + chaos.nextInt(shuffled.size() - 1);
                c.net.partition(Set.copyOf(shuffled.subList(0, cut)), Set.copyOf(shuffled.subList(cut, shuffled.size())));
                c.runFor(10 + chaos.nextInt(40));
                c.net.heal();
                c.runFor(chaos.nextInt(40));
            }
            assertTrue(c.electionSafetyViolations().isEmpty(), "seed " + seed + ": " + c.electionSafetyViolations());
            c.net.setDropRate(0);
            c.net.setDuplicateRate(0);
            c.net.setDelayRange(1, 1);
            c.awaitSingleLeader(500);
        }
    }
}
