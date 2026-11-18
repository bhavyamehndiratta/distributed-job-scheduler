package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import org.junit.jupiter.api.Test;

class ClientTest {

    @Test
    void aSubmittedJobIsAcceptedAndRuns() {
        SchedulerCluster c = new SchedulerCluster(3, 1, 11, new EffectStore());
        Client client = c.addClient("c1", 600);
        long runAt = c.nowMillis() + 2_000;
        Client.Call<Client.SubmitResult> call = client.submit("j", JobSpec.once("effect", "5", runAt, 3), "k1");
        assertTrue(c.runUntil(call::isDone, 1000));
        assertEquals(ClientMessages.Status.ACCEPTED, call.value().status());
        assertEquals("j", call.value().jobId());
        String run = "j@" + runAt;
        assertTrue(c.runUntil(() -> c.runState(run) == RunState.SUCCEEDED, 1500));
        assertEquals(1, c.store.effectCount(run));
    }

    @Test
    void theClientFollowsALeaderChange() {
        SchedulerCluster c = new SchedulerCluster(3, 1, 12, new EffectStore());
        Client client = c.addClient("c1", 1500);
        c.awaitLeader();
        c.crashNode(c.leader().id());
        long runAt = c.nowMillis() + 2_000;
        Client.Call<Client.SubmitResult> call = client.submit("j", JobSpec.once("effect", "5", runAt, 3), "k1");
        assertTrue(c.runUntil(call::isDone, 3000));
        assertFalse(call.isTimedOut());
        assertEquals(ClientMessages.Status.ACCEPTED, call.value().status());
    }

    @Test
    void theSameKeyTwiceGivesDuplicateWithTheOriginalJobId() {
        SchedulerCluster c = new SchedulerCluster(3, 1, 13, new EffectStore());
        Client client = c.addClient("c1", 600);
        long runAt = c.nowMillis() + 50_000;
        Client.Call<Client.SubmitResult> first = client.submit("a", JobSpec.once("effect", "5", runAt, 1), "same");
        assertTrue(c.runUntil(first::isDone, 1000));
        Client.Call<Client.SubmitResult> second = client.submit("b", JobSpec.once("effect", "5", runAt, 1), "same");
        assertTrue(c.runUntil(second::isDone, 1000));
        assertEquals(ClientMessages.Status.ACCEPTED, first.value().status());
        assertEquals(ClientMessages.Status.DUPLICATE, second.value().status());
        assertEquals("a", second.value().jobId());
        assertEquals(1, c.leader().stateMachine().jobs().size());
    }

    @Test
    void aLossyNetworkStillCreatesExactlyOneJob() {
        SchedulerCluster c = new SchedulerCluster(3, 1, 14, new EffectStore());
        Client client = c.addClient("c1", 4000);
        c.awaitLeader();
        c.net.setDropRate(0.3);
        long runAt = c.nowMillis() + 50_000;
        Client.Call<Client.SubmitResult> call = client.submit("j", JobSpec.once("effect", "5", runAt, 1), "k");
        assertTrue(c.runUntil(call::isDone, 4000));
        assertFalse(call.isTimedOut());
        c.net.setDropRate(0);
        c.runFor(300);
        c.awaitLeader();
        assertEquals(1, c.leader().stateMachine().jobs().size());
    }

    @Test
    void cancelAndGetJobWork() {
        SchedulerCluster c = new SchedulerCluster(3, 1, 15, new EffectStore());
        Client client = c.addClient("c1", 600);
        long runAt = c.nowMillis() + 600_000;
        Client.Call<Client.SubmitResult> submit = client.submit("j", JobSpec.once("effect", "5", runAt, 1), "k");
        assertTrue(c.runUntil(submit::isDone, 1000));
        Client.Call<Client.CancelResult> cancel = client.cancel("j");
        assertTrue(c.runUntil(cancel::isDone, 1000));
        assertEquals(ClientMessages.Status.ACCEPTED, cancel.value().status());
        Client.Call<Client.JobInfo> info = client.getJob("j");
        assertTrue(c.runUntil(info::isDone, 1000));
        assertTrue(info.value().found());
        assertEquals(JobStatus.CANCELLED, info.value().job().status());
        Client.Call<Client.JobInfo> missing = client.getJob("nope");
        assertTrue(c.runUntil(missing::isDone, 1000));
        assertFalse(missing.value().found());
    }

    @Test
    void aRequestWithNoReachableLeaderTimesOut() {
        SchedulerCluster c = new SchedulerCluster(3, 1, 16, new EffectStore());
        Client client = c.addClient("c1", 100);
        for (NodeId id : c.nodeIds()) {
            c.crashNode(id);
        }
        Client.Call<Client.SubmitResult> call = client.submit("j", JobSpec.once("effect", "5", 1000, 1), "k");
        assertTrue(c.runUntil(call::isDone, 500));
        assertTrue(call.isTimedOut());
    }
}
