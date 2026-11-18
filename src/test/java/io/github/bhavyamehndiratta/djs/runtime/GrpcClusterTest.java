package io.github.bhavyamehndiratta.djs.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.scheduler.Client;
import io.github.bhavyamehndiratta.djs.scheduler.ClientMessages;
import io.github.bhavyamehndiratta.djs.scheduler.EffectStore;
import io.github.bhavyamehndiratta.djs.scheduler.JobSpec;
import io.github.bhavyamehndiratta.djs.scheduler.RunState;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real processes' worth of machinery in one JVM: real gRPC on localhost, real files, real clock. */
class GrpcClusterTest {

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void eventually(String what, BooleanSupplier condition, long millis) throws Exception {
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private static boolean succeeded(RemoteClient client, String jobId) {
        Client.JobInfo info = client.getJob(jobId);
        return info.found() && !info.runs().isEmpty()
                && info.runs().stream().allMatch(r -> r.state() == RunState.SUCCEEDED);
    }

    @Test
    void aRealClusterRunsJobsAndSurvivesLosingItsLeader(@TempDir Path dir) throws Exception {
        Map<NodeId, String> cluster = new TreeMap<>();
        for (int i = 1; i <= 3; i++) {
            cluster.put(new NodeId("n" + i), "127.0.0.1:" + freePort());
        }
        Map<NodeId, SchedulerServer> servers = new TreeMap<>();
        WorkerProcess worker = null;
        RemoteClient client = null;
        try {
            for (NodeId id : cluster.keySet()) {
                servers.put(id, new SchedulerServer(id, cluster, dir.resolve(id.value()), "127.0.0.1", 50, 30));
            }
            worker = new WorkerProcess(new NodeId("w1"), cluster, "127.0.0.1", 0, 50, new EffectStore());
            client = new RemoteClient(cluster, "127.0.0.1", 50, 20_000);

            Client.SubmitResult first = client.submit("job1",
                    JobSpec.once("effect", "3", System.currentTimeMillis() + 500, 3), "k1");
            assertEquals(ClientMessages.Status.ACCEPTED, first.status());
            RemoteClient c1 = client;
            eventually("job1 to succeed", () -> succeeded(c1, "job1"), 30_000);
            String run1 = client.getJob("job1").runs().get(0).runId();
            assertEquals(1, worker.effectCount(run1));

            NodeId leaderId = null;
            for (var e : servers.entrySet()) {
                if (e.getValue().isLeader()) {
                    leaderId = e.getKey();
                }
            }
            assertTrue(leaderId != null, "no leader found");
            servers.remove(leaderId).close();

            Client.SubmitResult second = client.submit("job2",
                    JobSpec.once("effect", "3", System.currentTimeMillis() + 500, 3), "k2");
            assertEquals(ClientMessages.Status.ACCEPTED, second.status());
            eventually("job2 to succeed after the leader died", () -> succeeded(c1, "job2"), 40_000);
            String run2 = client.getJob("job2").runs().get(0).runId();
            assertEquals(1, worker.effectCount(run2));
            assertEquals(2, client.listJobs().size());
            assertEquals(ClientMessages.Status.DUPLICATE,
                    client.submit("job2", JobSpec.once("effect", "3", 1, 3), "k2").status());
        } finally {
            if (client != null) {
                client.close();
            }
            if (worker != null) {
                worker.close();
            }
            for (SchedulerServer s : servers.values()) {
                s.close();
            }
        }
    }
}
