package io.github.bhavyamehndiratta.djs.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimulatedNetworkTest {

    private record Ping(int n) implements Message { }

    private static final NodeId A = new NodeId("a");
    private static final NodeId B = new NodeId("b");
    private static final NodeId C = new NodeId("c");

    private SimulatedNetwork net;
    private List<String> received;

    @BeforeEach
    void setUp() {
        net = new SimulatedNetwork(42);
        received = new ArrayList<>();
        for (NodeId id : List.of(A, B, C)) {
            net.register(id, (from, msg) -> received.add(id + "<-" + from + ":" + ((Ping) msg).n()));
        }
    }

    @Test
    void deliversAfterMinimumDelay() {
        net.send(A, B, new Ping(1));
        assertTrue(received.isEmpty(), "nothing is delivered before time advances");
        net.step();
        assertEquals(List.of("b<-a:1"), received);
    }

    @Test
    void dropRateOneDropsEverything() {
        net.setDropRate(1.0);
        for (int i = 0; i < 20; i++) {
            net.send(A, B, new Ping(i));
        }
        net.runFor(10);
        assertTrue(received.isEmpty());
        assertEquals(20, net.droppedCount());
    }

    @Test
    void duplicateRateOneDeliversTwice() {
        net.setDuplicateRate(1.0);
        net.send(A, B, new Ping(7));
        net.runFor(5);
        assertEquals(List.of("b<-a:7", "b<-a:7"), received);
    }

    @Test
    void partitionBlocksAcrossGroupsAndAllowsWithin() {
        net.partition(Set.of(A, B), Set.of(C));
        net.send(A, B, new Ping(1));
        net.send(A, C, new Ping(2));
        net.runFor(5);
        assertEquals(List.of("b<-a:1"), received);
    }

    @Test
    void healRestoresConnectivity() {
        net.partition(Set.of(A), Set.of(B, C));
        net.send(A, B, new Ping(1));
        net.runFor(5);
        assertTrue(received.isEmpty());
        net.heal();
        net.send(A, B, new Ping(2));
        net.runFor(5);
        assertEquals(List.of("b<-a:2"), received);
    }

    @Test
    void partitionAppliedAtDeliveryDropsMessagesAlreadyInFlight() {
        net.setDelayRange(5, 5);
        net.send(A, B, new Ping(1));
        net.runFor(2);
        net.partition(Set.of(A), Set.of(B, C));
        net.runFor(10);
        assertTrue(received.isEmpty());
    }

    @Test
    void downNodeNeitherSendsNorReceives() {
        net.setNodeUp(B, false);
        net.send(A, B, new Ping(1));
        net.send(B, A, new Ping(2));
        net.runFor(5);
        assertTrue(received.isEmpty());
        net.setNodeUp(B, true);
        net.send(A, B, new Ping(3));
        net.runFor(5);
        assertEquals(List.of("b<-a:3"), received);
    }

    @Test
    void randomDelaysReorderMessages() {
        net.setDelayRange(1, 20);
        for (int i = 0; i < 50; i++) {
            net.send(A, B, new Ping(i));
        }
        net.runFor(30);
        assertEquals(50, received.size());
        List<String> inSendOrder = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            inSendOrder.add("b<-a:" + i);
        }
        assertNotEquals(inSendOrder, received, "random delays should reorder some messages");
    }

    @Test
    void sameSeedGivesIdenticalRun() {
        assertEquals(runScenario(123), runScenario(123));
    }

    @Test
    void differentSeedGivesDifferentRun() {
        assertNotEquals(runScenario(1), runScenario(2));
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> net.setDropRate(1.5));
        assertThrows(IllegalArgumentException.class, () -> net.setDelayRange(0, 3));
        assertThrows(IllegalArgumentException.class, () -> net.setDelayRange(5, 2));
        assertThrows(IllegalArgumentException.class, () -> new NodeId(" "));
    }

    private static List<String> runScenario(long seed) {
        SimulatedNetwork n = new SimulatedNetwork(seed);
        List<String> log = new ArrayList<>();
        for (NodeId id : List.of(A, B, C)) {
            n.register(id, (from, msg) -> log.add(n.now() + ":" + id + "<-" + from + ":" + ((Ping) msg).n()));
        }
        n.setDropRate(0.2);
        n.setDuplicateRate(0.2);
        n.setDelayRange(1, 10);
        List<NodeId> all = List.of(A, B, C);
        for (int i = 0; i < 100; i++) {
            n.send(all.get(i % 3), all.get((i + 1) % 3), new Ping(i));
            n.step();
        }
        n.runFor(30);
        return log;
    }
}
