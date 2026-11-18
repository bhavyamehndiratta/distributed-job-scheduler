package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SchedulerReadTest {

    private static boolean gotJobReply(List<Message> replies, long requestId) {
        for (Message m : replies) {
            if (m instanceof ClientMessages.JobReply r && r.requestId() == requestId) {
                return true;
            }
        }
        return false;
    }

    @Test
    void aReadIsAnsweredAfterTheLeaderIsConfirmed() {
        SchedulerCluster c = new SchedulerCluster(3, 0, 1, new EffectStore(true));
        c.awaitLeader();
        c.runFor(100);
        NodeId probe = new NodeId("probe");
        List<Message> replies = new ArrayList<>();
        c.net.register(probe, (from, msg) -> replies.add(msg));
        c.net.setNodeUp(probe, true);
        c.net.send(probe, c.leader().id(), new ClientMessages.GetJob(1, "nope"));
        c.runFor(30);
        assertTrue(gotJobReply(replies, 1), "no reply: " + replies);
    }

    @Test
    void aLeaderWithoutAMajorityNeverAnswersARead() {
        SchedulerCluster c = new SchedulerCluster(3, 0, 2, new EffectStore(true));
        c.awaitLeader();
        c.runFor(100);
        SchedulerNode leader = c.leader();
        for (NodeId n : c.nodeIds()) {
            if (!n.equals(leader.id())) {
                c.crashNode(n);
            }
        }
        NodeId probe = new NodeId("probe");
        List<Message> replies = new ArrayList<>();
        c.net.register(probe, (from, msg) -> replies.add(msg));
        c.net.setNodeUp(probe, true);
        c.net.send(probe, leader.id(), new ClientMessages.GetJob(1, "nope"));
        c.runFor(60);
        assertFalse(gotJobReply(replies, 1), "answered a read without a majority");
    }
}
