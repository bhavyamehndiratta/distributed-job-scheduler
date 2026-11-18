package io.github.bhavyamehndiratta.djs.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.scheduler.Client;
import io.github.bhavyamehndiratta.djs.scheduler.ClientMessages;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real processes' worth of machinery on localhost: gRPC, files on disk, wall clock. */
class GrpcMembershipTest {

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static boolean worked(Client.MembersResult r) {
        return r.status() == ClientMessages.Status.ACCEPTED || r.status() == ClientMessages.Status.DUPLICATE;
    }

    private static Client.MembersResult until(Supplier<Client.MembersResult> call, Predicate<Client.MembersResult> ok,
                                              String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 40_000;
        Client.MembersResult last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = call.get();
                if (ok.test(last)) {
                    return last;
                }
            } catch (IllegalStateException e) {
                // no leader answered yet
            }
            Thread.sleep(300);
        }
        throw new AssertionError("gave up waiting for " + what + ", last answer: " + last);
    }

    private static NodeId leaderOf(Map<NodeId, SchedulerServer> servers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (Map.Entry<NodeId, SchedulerServer> e : servers.entrySet()) {
                if (e.getValue().isLeader()) {
                    return e.getKey();
                }
            }
            Thread.sleep(200);
        }
        throw new AssertionError("no leader among " + servers.keySet());
    }

    @Test
    void aNodeJoinsIsPromotedAndTheClusterSurvivesLosingItsLeader(@TempDir Path dir) throws Exception {
        NodeId n4 = new NodeId("n4");
        Map<NodeId, String> three = new TreeMap<>();
        for (int i = 1; i <= 3; i++) {
            three.put(new NodeId("n" + i), "127.0.0.1:" + freePort());
        }
        Map<NodeId, String> four = new TreeMap<>(three);
        four.put(n4, "127.0.0.1:" + freePort());
        Map<NodeId, SchedulerServer> servers = new TreeMap<>();
        List<AutoCloseable> clients = new ArrayList<>();
        try {
            for (NodeId id : three.keySet()) {
                servers.put(id, new SchedulerServer(id, three, dir.resolve(id.value()), "127.0.0.1", 50, 30));
            }
            RemoteClient first = new RemoteClient(three, "127.0.0.1", 50, 15_000);
            clients.add(first);
            Client.MembersResult listed = until(() -> first.members("LIST", "", ""), r -> worked(r), "a leader");
            assertEquals("n1,n2,n3", listed.voters());

            servers.put(n4, new SchedulerServer(n4, four, dir.resolve("n4"), "127.0.0.1", 50, 30, true));
            Client.MembersResult added = until(() -> first.members("ADD", "n4", four.get(n4)), r -> worked(r),
                    "n4 to be added");
            assertTrue(added.learners().contains("n4"), added.toString());
            Client.MembersResult promoted = until(() -> first.members("PROMOTE", "n4", ""), r -> worked(r),
                    "n4 to be promoted");
            assertEquals("n1,n2,n3,n4", promoted.voters());
            assertEquals("", promoted.learners());

            NodeId dead = leaderOf(servers);
            servers.remove(dead).close();
            RemoteClient second = new RemoteClient(four, "127.0.0.1", 50, 15_000);
            clients.add(second);
            NodeId newLeader = leaderOf(servers);
            assertFalse(newLeader.equals(dead));
            Client.MembersResult afterFailover = until(() -> second.members("LIST", "", ""), r -> worked(r),
                    "the cluster to answer after losing " + dead.value());
            assertEquals("n1,n2,n3,n4", afterFailover.voters());

            Client.MembersResult removed = until(() -> second.members("REMOVE", dead.value(), ""), r -> worked(r),
                    "the dead node to be removed");
            assertFalse(removed.voters().contains(dead.value()), removed.toString());
            assertEquals(3, removed.voters().split(",").length);
        } finally {
            for (AutoCloseable c : clients) {
                c.close();
            }
            for (SchedulerServer s : servers.values()) {
                s.close();
            }
        }
    }
}
