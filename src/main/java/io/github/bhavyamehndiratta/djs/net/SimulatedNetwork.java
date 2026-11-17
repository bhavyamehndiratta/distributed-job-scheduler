package io.github.bhavyamehndiratta.djs.net;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 * Deterministic in-memory network for tests. Time is virtual: it only moves
 * when {@link #step()} is called. All randomness comes from one seeded
 * {@link Random}, so a run is reproduced exactly by reusing the seed.
 *
 * <p>Faults: random drop, random duplication, random delay (which also causes
 * reordering), partitions into groups, and nodes marked down. Not thread safe
 * by design: the simulator drives everything from one thread.
 */
public final class SimulatedNetwork implements Transport {

    private record Pending(long deliverAt, long seq, NodeId from, NodeId to, Message message) { }

    private final Random random;
    private final Map<NodeId, MessageHandler> handlers = new HashMap<>();
    private final Set<NodeId> down = new HashSet<>();
    private final PriorityQueue<Pending> queue = new PriorityQueue<>((a, b) -> {
        int byTime = Long.compare(a.deliverAt(), b.deliverAt());
        return byTime != 0 ? byTime : Long.compare(a.seq(), b.seq());
    });

    private Map<NodeId, Integer> partitionOf = new HashMap<>();
    private double dropRate = 0.0;
    private double duplicateRate = 0.0;
    private long minDelay = 1;
    private long maxDelay = 1;
    private long now = 0;
    private long nextSeq = 0;

    private long sent = 0;
    private long dropped = 0;
    private long delivered = 0;

    public SimulatedNetwork(long seed) {
        this.random = new Random(seed);
    }

    public void register(NodeId id, MessageHandler handler) {
        handlers.put(id, handler);
    }

    // ---- fault configuration ----

    public void setDropRate(double rate) {
        this.dropRate = requireProbability(rate);
    }

    public void setDuplicateRate(double rate) {
        this.duplicateRate = requireProbability(rate);
    }

    /** Delay is drawn uniformly from [min, max] ticks. Min must be at least 1. */
    public void setDelayRange(long min, long max) {
        if (min < 1 || max < min) {
            throw new IllegalArgumentException("need 1 <= min <= max");
        }
        this.minDelay = min;
        this.maxDelay = max;
    }

    /** Split the cluster. Nodes can only talk to nodes in their own group. */
    @SafeVarargs
    public final void partition(Set<NodeId>... groups) {
        Map<NodeId, Integer> map = new HashMap<>();
        for (int i = 0; i < groups.length; i++) {
            for (NodeId id : groups[i]) {
                map.put(id, i);
            }
        }
        this.partitionOf = map;
    }

    /** Remove all partitions. */
    public void heal() {
        this.partitionOf = new HashMap<>();
    }

    /** A node that is down neither sends nor receives. In-flight messages to it are lost. */
    public void setNodeUp(NodeId id, boolean up) {
        if (up) {
            down.remove(id);
        } else {
            down.add(id);
        }
    }

    // ---- Transport ----

    @Override
    public void send(NodeId from, NodeId to, Message message) {
        sent++;
        if (down.contains(from) || random.nextDouble() < dropRate) {
            dropped++;
            return;
        }
        enqueue(from, to, message);
        if (random.nextDouble() < duplicateRate) {
            enqueue(from, to, message);
        }
    }

    private void enqueue(NodeId from, NodeId to, Message message) {
        long delay = minDelay + (maxDelay > minDelay ? random.nextLong(maxDelay - minDelay + 1) : 0);
        queue.add(new Pending(now + delay, nextSeq++, from, to, message));
    }

    // ---- virtual time ----

    public long now() {
        return now;
    }

    /** Advance virtual time by one tick and deliver every message that is now due. */
    public void step() {
        now++;
        while (!queue.isEmpty() && queue.peek().deliverAt() <= now) {
            Pending p = queue.poll();
            if (down.contains(p.to()) || !connected(p.from(), p.to())) {
                dropped++;
                continue;
            }
            MessageHandler handler = handlers.get(p.to());
            if (handler == null) {
                dropped++;
                continue;
            }
            delivered++;
            handler.onMessage(p.from(), p.message());
        }
    }

    public void runFor(long ticks) {
        for (long i = 0; i < ticks; i++) {
            step();
        }
    }

    private boolean connected(NodeId a, NodeId b) {
        if (partitionOf.isEmpty()) {
            return true;
        }
        Integer ga = partitionOf.get(a);
        Integer gb = partitionOf.get(b);
        return ga != null && ga.equals(gb);
    }

    // ---- stats ----

    public long sentCount() {
        return sent;
    }

    public long droppedCount() {
        return dropped;
    }

    public long deliveredCount() {
        return delivered;
    }

    public int inFlight() {
        return queue.size();
    }

    private static double requireProbability(double p) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("probability must be in [0, 1]");
        }
        return p;
    }
}
