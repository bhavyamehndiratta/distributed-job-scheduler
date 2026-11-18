package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LogReplicationTest {

    private static final NodeId A = new NodeId("a");
    private static final NodeId B = new NodeId("b");
    private static final NodeId C = new NodeId("c");

    // ---- cluster level ----

    @Test
    void newLeaderAppendsAndCommitsANoOp() {
        TestCluster c = new TestCluster(3, 1);
        RaftNode leader = c.awaitSingleLeader(200);
        c.runFor(30);
        assertTrue(c.log(leader.id()).get(1).isNoop());
        for (NodeId id : c.members()) {
            assertTrue(c.node(id).commitIndex() >= 1, id + " should have committed the no-op");
        }
    }

    @Test
    void proposedCommandIsReplicatedCommittedAndAppliedEverywhere() {
        TestCluster c = new TestCluster(3, 2);
        RaftNode leader = c.awaitSingleLeader(200);
        long index = leader.propose("job-1");
        assertTrue(index > 0);
        c.runFor(30);
        for (NodeId id : c.members()) {
            assertEquals("job-1", c.log(id).get(index).command());
            assertTrue(c.node(id).commitIndex() >= index);
            assertEquals(c.node(id).commitIndex(), c.node(id).lastApplied());
        }
        assertEquals("job-1", c.appliedEntries().get(index).command());
    }

    @Test
    void followerRefusesProposals() {
        TestCluster c = new TestCluster(3, 3);
        RaftNode leader = c.awaitSingleLeader(200);
        NodeId follower = c.members().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        assertEquals(-1, c.node(follower).propose("nope"));
    }

    @Test
    void leaderCutOffInTheMinorityCannotCommit() {
        TestCluster c = new TestCluster(5, 4);
        RaftNode leader = c.awaitSingleLeader(200);
        c.runFor(30);
        long before = leader.commitIndex();
        List<NodeId> others = new ArrayList<>(c.members());
        others.remove(leader.id());
        c.net.partition(Set.of(leader.id(), others.get(0)), Set.copyOf(others.subList(1, 4)));
        long index = leader.propose("must-not-commit");
        c.runFor(40);
        assertTrue(leader.lastLogIndex() >= index);
        assertEquals(before, leader.commitIndex(), "two of five is not a majority");
    }

    @Test
    void farBehindFollowerCatchesUpInBatchesAfterHealing() {
        TestCluster c = new TestCluster(3, 5);
        RaftNode leader = c.awaitSingleLeader(200);
        NodeId lagging = c.members().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        Set<NodeId> rest = new HashSet<>(c.members());
        rest.remove(lagging);
        c.net.partition(Set.of(lagging), rest);
        for (int i = 0; i < 200; i++) {
            leader.propose("cmd-" + i);
            c.runFor(1);
        }
        c.runFor(50);
        assertTrue(c.log(lagging).lastIndex() < 10, "cut off, so it should hold almost nothing");
        c.net.heal();
        c.runFor(300);
        assertEquals(leader.lastLogIndex(), c.log(lagging).lastIndex());
        assertEquals(leader.commitIndex(), c.node(lagging).commitIndex());
        assertTrue(c.logMatchingViolations().isEmpty());
    }

    @Test
    void conflictingUncommittedEntriesFromAnOldLeaderAreOverwritten() {
        TestCluster c = new TestCluster(5, 6);
        RaftNode oldLeader = c.awaitSingleLeader(200);
        c.runFor(30);
        List<NodeId> others = new ArrayList<>(c.members());
        others.remove(oldLeader.id());
        Set<NodeId> minority = Set.of(oldLeader.id(), others.get(0));
        Set<NodeId> majority = Set.copyOf(others.subList(1, 4));
        c.net.partition(minority, majority);
        oldLeader.propose("doomed-1");
        oldLeader.propose("doomed-2");
        c.runFor(200);
        RaftNode newLeader = c.leaders().stream().filter(n -> majority.contains(n.id())).findFirst().orElseThrow();
        long kept = newLeader.propose("survivor");
        c.runFor(50);
        c.net.heal();
        c.runFor(300);
        for (NodeId id : c.members()) {
            for (long i = 1; i <= c.log(id).lastIndex(); i++) {
                String command = c.log(id).get(i).command();
                assertNotEquals("doomed-1", command, id + " still holds a doomed entry");
                assertNotEquals("doomed-2", command, id + " still holds a doomed entry");
            }
            assertEquals("survivor", c.log(id).get(kept).command());
        }
        assertTrue(c.stateMachineViolations().isEmpty());
        assertTrue(c.logMatchingViolations().isEmpty());
    }

    @Test
    void committedEntriesSurviveALeaderCrash() {
        TestCluster c = new TestCluster(3, 7);
        RaftNode first = c.awaitSingleLeader(200);
        long x = first.propose("x");
        c.runFor(30);
        assertTrue(first.commitIndex() >= x);
        c.crash(first.id());
        RaftNode second = c.awaitSingleLeader(200);
        long y = second.propose("y");
        c.runFor(30);
        c.restart(first.id());
        c.runFor(100);
        for (NodeId id : c.members()) {
            assertEquals("x", c.log(id).get(x).command());
            assertEquals("y", c.log(id).get(y).command());
        }
        assertTrue(c.stateMachineViolations().isEmpty());
    }

    // ---- single node level ----

    private static RaftNode follower(InMemoryRaftLog log, long term, List<Message> sent) {
        InMemoryPersistentState state = new InMemoryPersistentState();
        if (term > 0) {
            state.setTermAndVote(term, null);
        }
        return new RaftNode(A, List.of(A, B, C), (from, to, msg) -> sent.add(msg), new Random(1),
                RaftConfig.defaults(), state, log, StateMachine.NONE, RaftObserver.NONE);
    }

    private static InMemoryRaftLog logWithTerms(long... terms) {
        InMemoryRaftLog log = new InMemoryRaftLog();
        for (int i = 0; i < terms.length; i++) {
            log.append(new LogEntry(terms[i], "e" + (i + 1)));
        }
        return log;
    }

    @Test
    void leaderDoesNotCommitAnOlderTermEntryJustByCountingReplicas() {
        List<Message> sent = new ArrayList<>();
        InMemoryPersistentState state = new InMemoryPersistentState();
        state.setTermAndVote(3, null);
        InMemoryRaftLog log = logWithTerms(1, 2);
        RaftNode node = new RaftNode(A, List.of(A, B, C), (from, to, msg) -> sent.add(msg), new Random(1),
                RaftConfig.defaults(), state, log, StateMachine.NONE, RaftObserver.NONE);
        for (int i = 0; i < 40 && node.role() != Role.PRE_CANDIDATE; i++) {
            node.tick();
        }
        assertEquals(Role.PRE_CANDIDATE, node.role());
        node.handle(B, new RequestVoteResponse(4, true, true));
        node.handle(B, new RequestVoteResponse(4, true, false));
        assertEquals(Role.LEADER, node.role());
        assertEquals(3, node.lastLogIndex(), "the no-op sits at index 3, in term 4");

        node.handle(B, new AppendEntriesResponse(4, true, 2, 0, 0));
        assertEquals(0, node.commitIndex(),
                "the term-2 entry is on a majority, but a term-4 leader must not commit it by counting");
        node.handle(B, new AppendEntriesResponse(4, true, 3, 0, 0));
        assertEquals(3, node.commitIndex(), "committing the term-4 no-op commits everything before it");
    }

    @Test
    void oldDuplicateAppendEntriesDoesNotTruncateTheLog() {
        List<Message> sent = new ArrayList<>();
        InMemoryRaftLog log = logWithTerms(1, 1, 1);
        RaftNode node = follower(log, 1, sent);
        node.handle(B, new AppendEntries(1, B, 0, 0, List.of(new LogEntry(1, "e1")), 0));
        assertEquals(3, log.lastIndex(), "a stale message that matches what we hold must change nothing");
        assertEquals(new AppendEntriesResponse(1, true, 1, 0, 0), sent.get(sent.size() - 1));
    }

    @Test
    void followerReportsWhereItsLogConflicts() {
        List<Message> sent = new ArrayList<>();
        RaftNode node = follower(logWithTerms(1, 1, 2, 2), 2, sent);
        node.handle(B, new AppendEntries(2, B, 4, 3, List.of(), 0));
        assertEquals(new AppendEntriesResponse(2, false, 0, 2, 3), sent.get(sent.size() - 1),
                "term 2 starts at index 3");
        node.handle(B, new AppendEntries(2, B, 9, 3, List.of(), 0));
        assertEquals(new AppendEntriesResponse(2, false, 0, 0, 5), sent.get(sent.size() - 1),
                "log too short: resume right after index 4");
    }

    @Test
    void followerReplacesAConflictingSuffix() {
        List<Message> sent = new ArrayList<>();
        InMemoryRaftLog log = logWithTerms(1, 1, 2, 2);
        RaftNode node = follower(log, 2, sent);
        node.handle(B, new AppendEntries(3, B, 2, 1, List.of(new LogEntry(3, "new")), 0));
        assertEquals(3, log.lastIndex());
        assertEquals(1, log.termAt(2));
        assertEquals("new", log.get(3).command());
        assertEquals(new AppendEntriesResponse(3, true, 3, 0, 0), sent.get(sent.size() - 1));
    }

    @Test
    void followerNeverCommitsBeyondWhatItHasFromThisMessage() {
        List<Message> sent = new ArrayList<>();
        RaftNode node = follower(new InMemoryRaftLog(), 1, sent);
        node.handle(B, new AppendEntries(1, B, 0, 0, List.of(new LogEntry(1, "e1")), 5));
        assertEquals(1, node.commitIndex(), "leader commit 5, but we only hold index 1");
    }

    // ---- chaos ----

    @Test
    void randomizedFaultsKeepEveryRaftSafetyProperty() {
        for (long seed = 1; seed <= 100; seed++) {
            TestCluster c = new TestCluster(5, seed);
            Random chaos = new Random(seed * 7919);
            c.net.setDropRate(0.1);
            c.net.setDuplicateRate(0.05);
            c.net.setDelayRange(1, 4);
            int proposals = 0;
            for (int round = 0; round < 60; round++) {
                int action = chaos.nextInt(10);
                if (action <= 1) {
                    List<NodeId> shuffled = new ArrayList<>(c.members());
                    Collections.shuffle(shuffled, chaos);
                    int cut = 1 + chaos.nextInt(shuffled.size() - 1);
                    c.net.partition(Set.copyOf(shuffled.subList(0, cut)),
                            Set.copyOf(shuffled.subList(cut, shuffled.size())));
                } else if (action == 2) {
                    c.net.heal();
                } else if (action == 3) {
                    List<NodeId> alive = c.upNodes();
                    if (alive.size() > 1) {
                        c.crash(alive.get(chaos.nextInt(alive.size())));
                    }
                } else if (action == 4) {
                    List<NodeId> dead = new ArrayList<>(c.members());
                    dead.removeIf(c::isUp);
                    if (!dead.isEmpty()) {
                        c.restart(dead.get(chaos.nextInt(dead.size())));
                    }
                } else {
                    for (int k = 1 + chaos.nextInt(3); k > 0; k--) {
                        c.propose("cmd-" + seed + "-" + proposals++);
                    }
                }
                c.runFor(5 + chaos.nextInt(30));
                assertTrue(c.electionSafetyViolations().isEmpty(), "seed " + seed + " election: " + c.electionSafetyViolations());
                assertTrue(c.stateMachineViolations().isEmpty(), "seed " + seed + " state machine: " + c.stateMachineViolations());
                assertTrue(c.logMatchingViolations().isEmpty(), "seed " + seed + " log matching: " + c.logMatchingViolations());
            }

            c.net.heal();
            c.net.setDropRate(0);
            c.net.setDuplicateRate(0);
            c.net.setDelayRange(1, 1);
            for (NodeId id : c.members()) {
                if (!c.isUp(id)) {
                    c.restart(id);
                }
            }
            c.awaitSingleLeader(1000);
            c.runFor(300);
            assertEquals(1, c.leaders().size(), "seed " + seed + ": one leader after healing");
            RaftNode leader = c.leaders().get(0);

            for (Map.Entry<Long, LogEntry> e : c.appliedEntries().entrySet()) {
                assertTrue(e.getKey() <= leader.lastLogIndex(), "seed " + seed + ": acknowledged entry " + e.getKey() + " is missing");
                assertEquals(e.getValue(), c.log(leader.id()).get(e.getKey()),
                        "seed " + seed + ": acknowledged entry " + e.getKey() + " was lost or changed");
            }
            for (NodeId id : c.members()) {
                assertEquals(leader.commitIndex(), c.node(id).commitIndex(), "seed " + seed + ": " + id + " commit index");
            }
            assertTrue(c.electionSafetyViolations().isEmpty(), "seed " + seed);
            assertTrue(c.stateMachineViolations().isEmpty(), "seed " + seed);
            assertTrue(c.logMatchingViolations().isEmpty(), "seed " + seed);
            assertFalse(c.appliedEntries().isEmpty(), "seed " + seed + ": nothing was ever committed");
        }
    }
}
