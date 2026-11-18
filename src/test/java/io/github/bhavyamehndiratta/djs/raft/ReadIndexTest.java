package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

class ReadIndexTest {

    @Test
    void aReadCompletesOnceAMajorityConfirmsLeadership() {
        TestCluster c = new TestCluster(3, 1);
        RaftNode leader = c.awaitSingleLeader(500);
        c.runFor(60);
        assertTrue(leader.beginRead(1));
        boolean ready = false;
        for (int i = 0; i < 30 && !ready; i++) {
            c.tick();
            ready = leader.readyReads().contains(1L);
        }
        assertTrue(ready, "read never became ready");
    }

    @Test
    void aLeaderCutOffFromItsMajorityNeverCompletesAread() {
        TestCluster c = new TestCluster(3, 2);
        RaftNode leader = c.awaitSingleLeader(500);
        c.runFor(60);
        for (NodeId n : c.members()) {
            if (!n.equals(leader.id())) {
                c.crash(n);
            }
        }
        assertTrue(leader.beginRead(7));
        for (int i = 0; i < 60; i++) {
            c.tick();
            assertFalse(leader.readyReads().contains(7L), "served a read without a majority");
        }
    }

    @Test
    void readsAreRefusedByFollowersAndByALeaderThatHasNotCommittedInItsTerm() {
        TestCluster c = new TestCluster(3, 3);
        RaftNode leader = c.awaitSingleLeader(500);
        assertFalse(leader.beginRead(1), "no entry of this term is committed yet");
        c.runFor(60);
        for (NodeId n : c.members()) {
            if (!n.equals(leader.id())) {
                assertFalse(c.node(n).beginRead(2), "a follower must refuse");
            }
        }
        assertTrue(leader.beginRead(3));
    }

    @Test
    void aDeposedLeaderNeverServesAStaleRead() {
        TestCluster c = new TestCluster(3, 4);
        RaftNode old = c.awaitSingleLeader(500);
        c.runFor(60);
        Set<NodeId> rest = new TreeSet<>(c.members());
        rest.remove(old.id());
        c.net.partition(Set.of(old.id()), rest);
        assertTrue(old.beginRead(5), "the old leader still believes it leads");
        for (int i = 0; i < 300; i++) {
            c.tick();
            assertFalse(old.readyReads().contains(5L), "a partitioned old leader served a read");
        }
        boolean otherLeader = false;
        for (RaftNode l : c.leaders()) {
            otherLeader |= !l.id().equals(old.id());
        }
        assertTrue(otherLeader, "the majority side should have elected a new leader");
        assertFalse(old.beginRead(6), "the old leader must have stepped down");
        c.net.heal();
        c.runFor(100);
        RaftNode now = c.awaitSingleLeader(300);
        assertTrue(now.beginRead(7));
    }
}
