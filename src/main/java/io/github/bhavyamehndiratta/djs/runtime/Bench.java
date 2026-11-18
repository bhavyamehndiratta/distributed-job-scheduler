package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.scheduler.EffectStore;
import io.github.bhavyamehndiratta.djs.scheduler.Handler;
import io.github.bhavyamehndiratta.djs.scheduler.JobSpec;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/**
 * Benchmarks on real machinery: gRPC on localhost, files on disk with fsync, wall clock, one driver
 * thread per node and per worker. All nodes, workers and clients share one JVM, which is stated in the
 * results. Usage: {@code bin/djs bench [--nodes 3,5] [--jobs 3000] [--workers 32] [--clients 16]
 * [--rate 100] [--seconds 20] [--trials 10] [--tick-ms 20] [--lease-ticks 30]}.
 *
 * <p>Latency is measured at a fixed submit rate below saturation, from the moment a job was due to be
 * submitted (not when the client got around to it, which would hide queueing) to the moment a worker
 * starts executing it. Throughput is measured separately by flooding.
 */
public final class Bench {

    private static final String HOST = "127.0.0.1";

    private Bench() {
    }

    /** The job body. Records when execution starts and plays the outside world with one shared effect store. */
    static final class BenchHandler implements Handler {
        private final EffectStore world = new EffectStore();
        final ConcurrentHashMap<String, Long> startNanos = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Character, AtomicInteger> done = new ConcurrentHashMap<>();

        @Override
        public int durationTicks(String args) {
            String[] p = args.split("\\|", 3);
            if (p.length == 3 && p[0].equals("m")) {
                startNanos.putIfAbsent(p[2], System.nanoTime() - Long.parseLong(p[1]));
            }
            return 1;
        }

        @Override
        public Result run(String runId, long token, String args, EffectStore ignored) {
            EffectStore.Result r;
            synchronized (world) {
                r = world.apply(runId, token);
            }
            String[] p = args.split("\\|", 3);
            if (r == EffectStore.Result.APPLIED && p.length == 3 && p[0].equals("m")) {
                done.computeIfAbsent(p[2].charAt(0), k -> new AtomicInteger()).incrementAndGet();
            }
            return r == EffectStore.Result.STALE ? Result.STALE : Result.SUCCESS;
        }

        int done(char phase) {
            AtomicInteger n = done.get(phase);
            return n == null ? 0 : n.get();
        }

        synchronized int duplicates() {
            synchronized (world) {
                return world.duplicateEffects().size();
            }
        }
    }

    private static final class Cluster implements AutoCloseable {
        final Map<NodeId, String> spec = new TreeMap<>();
        final Map<NodeId, SchedulerServer> servers = new TreeMap<>();
        final List<WorkerProcess> workers = new ArrayList<>();
        final List<RemoteClient> clients = new ArrayList<>();
        final BenchHandler handler = new BenchHandler();
        final Path dir;
        final long tickMs;

        Cluster(int nodes, int workerCount, long tickMs, long leaseTicks) throws IOException {
            this.tickMs = tickMs;
            this.dir = Files.createTempDirectory("djs-bench");
            for (int i = 1; i <= nodes; i++) {
                spec.put(new NodeId("n" + i), HOST + ":" + freePort());
            }
            for (NodeId id : spec.keySet()) {
                servers.put(id, new SchedulerServer(id, spec, dir.resolve(id.value()), HOST, tickMs, leaseTicks));
            }
            for (int i = 1; i <= workerCount; i++) {
                workers.add(new WorkerProcess(new NodeId("w" + i), spec, HOST, 0, tickMs, new EffectStore(),
                        Map.of("bench", handler)));
            }
        }

        RemoteClient newClient() throws IOException {
            RemoteClient c = new RemoteClient(spec, HOST, tickMs, 30_000);
            clients.add(c);
            return c;
        }

        NodeId awaitLeader(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (Map.Entry<NodeId, SchedulerServer> e : new ArrayList<>(servers.entrySet())) {
                    if (e.getValue().isLeader()) {
                        return e.getKey();
                    }
                }
                Thread.sleep(5);
            }
            throw new IllegalStateException("no leader within " + timeoutMs + " ms");
        }

        void kill(NodeId id) {
            servers.remove(id).close();
        }

        @Override
        public void close() {
            for (RemoteClient c : clients) {
                try {
                    c.close();
                } catch (RuntimeException e) {
                    // best effort
                }
            }
            for (WorkerProcess w : workers) {
                w.close();
            }
            for (SchedulerServer s : servers.values()) {
                s.close();
            }
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            } catch (IOException e) {
                // temp dir, leave it
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static boolean submit(RemoteClient client, String prefix, int k, long intendedNanos) {
        String jobId = prefix + k;
        try {
            JobSpec spec = JobSpec.once("bench", "m|" + intendedNanos + "|" + jobId, System.currentTimeMillis(), 5);
            return !String.valueOf(client.submit(jobId, spec, jobId)).contains("REJECTED");
        } catch (IllegalStateException e) {
            return false;
        }
    }

    /** Waits until {@code target} jobs of this phase have taken effect, or the timeout. Returns the finish time. */
    private static long waitDone(Cluster c, char phase, int target, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (c.handler.done(phase) < target && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        return System.nanoTime();
    }

    private record Flood(int acked, int completed, int errors, double submitSeconds, double totalSeconds) {
    }

    private static Flood flood(Cluster c, char prefix, int jobs, int threads) throws Exception {
        AtomicInteger next = new AtomicInteger();
        AtomicInteger acked = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        List<RemoteClient> cl = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            cl.add(c.newClient());
        }
        long t0 = System.nanoTime();
        List<Thread> ts = new ArrayList<>();
        for (RemoteClient client : cl) {
            Thread th = new Thread(() -> {
                int k;
                while ((k = next.getAndIncrement()) < jobs) {
                    if (submit(client, String.valueOf(prefix), k, System.nanoTime())) {
                        acked.incrementAndGet();
                    } else {
                        errors.incrementAndGet();
                    }
                }
            });
            th.start();
            ts.add(th);
        }
        for (Thread th : ts) {
            th.join();
        }
        long submitEnd = System.nanoTime();
        long end = waitDone(c, prefix, acked.get(), 120_000);
        return new Flood(acked.get(), c.handler.done(prefix), errors.get(), (submitEnd - t0) / 1e9, (end - t0) / 1e9);
    }

    private record Latency(int acked, int completed, int errors, double p50, double p99, double max) {
    }

    private static Latency paced(Cluster c, char prefix, int rate, int seconds, int threads) throws Exception {
        int total = rate * seconds;
        long interval = 1_000_000_000L / rate;
        long start = System.nanoTime() + 200_000_000L;
        AtomicInteger acked = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            RemoteClient client = c.newClient();
            int first = t;
            Thread th = new Thread(() -> {
                for (int k = first; k < total; k += threads) {
                    long due = start + k * interval;
                    long now;
                    while ((now = System.nanoTime()) < due) {
                        LockSupport.parkNanos(due - now);
                    }
                    // the latency clock starts at the intended time, so a slow submit cannot hide queueing
                    if (submit(client, String.valueOf(prefix), k, due)) {
                        acked.incrementAndGet();
                    } else {
                        errors.incrementAndGet();
                    }
                }
            });
            th.start();
            ts.add(th);
        }
        for (Thread th : ts) {
            th.join();
        }
        waitDone(c, prefix, acked.get(), 60_000);
        List<Double> ms = new ArrayList<>();
        for (Map.Entry<String, Long> e : c.handler.startNanos.entrySet()) {
            if (e.getKey().charAt(0) == prefix) {
                ms.add(e.getValue() / 1e6);
            }
        }
        Collections.sort(ms);
        return new Latency(acked.get(), c.handler.done(prefix), errors.get(), pct(ms, 50), pct(ms, 99),
                ms.isEmpty() ? Double.NaN : ms.get(ms.size() - 1));
    }

    private static double pct(List<Double> sorted, double p) {
        if (sorted.isEmpty()) {
            return Double.NaN;
        }
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }

    private static String f(double v) {
        return String.format(Locale.ROOT, "%.1f", v);
    }

    private static String sysctl(String key) {
        try {
            Process p = new ProcessBuilder("sysctl", "-n", key).redirectErrorStream(true).start();
            String s = new String(p.getInputStream().readAllBytes()).trim();
            p.waitFor();
            return s;
        } catch (Exception e) {
            return "unknown";
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = Cli.options(args, 0);
        List<Integer> sizes = new ArrayList<>();
        for (String s : o.getOrDefault("nodes", "3,5").split(",")) {
            sizes.add(Integer.parseInt(s.trim()));
        }
        int jobs = Integer.parseInt(o.getOrDefault("jobs", "3000"));
        int workers = Integer.parseInt(o.getOrDefault("workers", "32"));
        int clients = Integer.parseInt(o.getOrDefault("clients", "16"));
        int rate = Integer.parseInt(o.getOrDefault("rate", "100"));
        int seconds = Integer.parseInt(o.getOrDefault("seconds", "20"));
        int trials = Integer.parseInt(o.getOrDefault("trials", "10"));
        long tickMs = Long.parseLong(o.getOrDefault("tick-ms", "20"));
        long leaseTicks = Long.parseLong(o.getOrDefault("lease-ticks", "30"));

        String mem = sysctl("hw.memsize");
        System.out.println("BENCH HARDWARE: " + sysctl("machdep.cpu.brand_string") + ", "
                + Runtime.getRuntime().availableProcessors() + " cores, "
                + (mem.matches("\\d+") ? Long.parseLong(mem) / (1024L * 1024 * 1024) + " GB RAM" : "RAM unknown")
                + ", " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                + System.getProperty("os.arch") + ", Java " + System.getProperty("java.version")
                + ", max heap " + Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB");
        System.out.println("BENCH SETUP: real gRPC on localhost, FileRaftLog with group commit (one fsync per batch, outgoing messages held until it completes), all nodes, "
                + workers + " workers and clients in one JVM; tick " + tickMs + " ms (election timeout "
                + 10 * tickMs + " to " + 19 * tickMs + " ms, heartbeat " + 2 * tickMs + " ms), job body is one tick");

        for (int nodes : sizes) {
            System.out.println("== " + nodes + " nodes ==");
            try (Cluster c = new Cluster(nodes, workers, tickMs, leaseTicks)) {
                c.awaitLeader(20_000);
                Thread.sleep(1000);
                flood(c, 'w', 300, 8); // warm up the JIT and the connections; not reported
                Thread.sleep(500);
                Latency lat = paced(c, 'l', rate, seconds, Math.min(clients, 8));
                System.out.println("BENCH LATENCY " + nodes + " nodes: fixed rate " + rate + " jobs/s for " + seconds
                        + " s: " + lat.acked() + " acked, " + lat.completed() + " executed, " + lat.errors()
                        + " errors; submit-to-execution-start p50 " + f(lat.p50()) + " ms, p99 " + f(lat.p99())
                        + " ms, max " + f(lat.max()) + " ms");
                Thread.sleep(500);
                Flood fl = flood(c, 'f', jobs, clients);
                System.out.println("BENCH THROUGHPUT " + nodes + " nodes: " + jobs + " jobs from " + clients
                        + " client threads, " + workers + " workers: " + fl.acked() + " acked in "
                        + f(fl.submitSeconds()) + " s (" + f(fl.acked() / fl.submitSeconds()) + " committed submits/s), "
                        + fl.completed() + " executed in " + f(fl.totalSeconds()) + " s ("
                        + f(fl.completed() / fl.totalSeconds()) + " jobs/s end to end), " + fl.errors() + " errors");
                int lost = (fl.acked() - fl.completed()) + (lat.acked() - lat.completed());
                System.out.println("BENCH INTEGRITY " + nodes + " nodes: " + lost + " acked jobs not executed, "
                        + c.handler.duplicates() + " duplicate effects");
            }
            List<Double> elect = new ArrayList<>();
            List<Double> write = new ArrayList<>();
            for (int t = 0; t < trials; t++) {
                try (Cluster c = new Cluster(nodes, 0, tickMs, leaseTicks)) {
                    NodeId leader = c.awaitLeader(20_000);
                    RemoteClient cl = c.newClient();
                    long farFuture = System.currentTimeMillis() + 3_600_000L;
                    cl.submit("warm" + t, JobSpec.once("bench", "x|0|warm", farFuture, 5), "warm" + t);
                    Thread.sleep(500);
                    c.kill(leader);
                    long t0 = System.nanoTime();
                    long[] wrote = {0};
                    int trial = t;
                    Thread w = new Thread(() -> {
                        try {
                            cl.submit("fo" + trial, JobSpec.once("bench", "x|0|fo", farFuture, 5), "fo" + trial);
                            wrote[0] = System.nanoTime();
                        } catch (IllegalStateException e) {
                            wrote[0] = -1;
                        }
                    });
                    w.start();
                    c.awaitLeader(30_000);
                    long elected = System.nanoTime();
                    w.join();
                    if (wrote[0] > 0) {
                        elect.add((elected - t0) / 1e6);
                        write.add((wrote[0] - t0) / 1e6);
                    }
                }
            }
            Collections.sort(elect);
            Collections.sort(write);
            System.out.println("BENCH FAILOVER " + nodes + " nodes: " + elect.size() + " of " + trials
                    + " trials; kill leader to new leader: median " + f(pct(elect, 50)) + " ms, p95 " + f(pct(elect, 95))
                    + " ms, max " + f(elect.isEmpty() ? Double.NaN : elect.get(elect.size() - 1))
                    + " ms; kill leader to first committed write: median " + f(pct(write, 50)) + " ms, p95 "
                    + f(pct(write, 95)) + " ms, max " + f(write.isEmpty() ? Double.NaN : write.get(write.size() - 1)) + " ms");
        }
        System.out.println("BENCH DONE");
        System.exit(0);
    }
}
