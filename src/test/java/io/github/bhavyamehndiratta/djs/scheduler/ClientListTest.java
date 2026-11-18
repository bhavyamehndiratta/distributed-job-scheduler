package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ClientListTest {

    @Test
    void listJobsReturnsEverySubmittedJob() {
        SchedulerCluster c = new SchedulerCluster(3, 1, 21, new EffectStore());
        Client client = c.addClient("c1", 600);
        long runAt = c.nowMillis() + 600_000;
        for (String id : new String[] {"a", "b"}) {
            Client.Call<Client.SubmitResult> s = client.submit(id, JobSpec.once("effect", "5", runAt, 1), "k-" + id);
            assertTrue(c.runUntil(s::isDone, 1000));
        }
        Client.Call<java.util.List<JobView>> list = client.listJobs();
        assertTrue(c.runUntil(list::isDone, 1000));
        assertEquals(2, list.value().size());
    }
}
