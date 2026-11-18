package io.github.bhavyamehndiratta.djs.scheduler;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.net.Transport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A worker. Like the Raft node it is single threaded and driven by tick() and
 * handle(), so the simulator can crash it, pause it, or delay its messages
 * deterministically. It holds no scheduling state: when idle it polls the
 * leader, runs the job it is given under the fencing token it was handed,
 * heartbeats while running, writes its effect through the effect store, and
 * reports back. If the leader says its token is stale it drops the run.
 *
 * <p>{@link #pause} models a frozen process (a long GC pause or a stalled VM):
 * nothing runs and incoming messages wait until it wakes up.
 */
public final class Worker {

    private static final int POLL_INTERVAL_TICKS = 5;
    private static final int LEADER_SILENCE_TICKS = 20;
    private static final int REPORT_RETRY_TICKS = 10;

    private enum Phase { RUNNING, REPORTING }

    private static final class Job {
        String runId;
        long token;
        String handlerName;
        String args;
        int remaining;
        long leaseTicks;
        int sinceHeartbeat = 0;
        int sinceReport = 0;
        Phase phase = Phase.RUNNING;
        boolean success;
        String detail = "";
    }

    private record Delivery(NodeId from, Message message) {
    }

    private final NodeId id;
    private final List<NodeId> schedulers;
    private final Transport transport;
    private final EffectStore store;
    private final Map<String, Handler> handlers;
    private int leaderGuess = 0;
    private long tick = 0;
    private long lastHeard = 0;
    private int sincePoll = 0;
    private int pausedTicks = 0;
    private final List<Delivery> queued = new ArrayList<>();
    private Job job = null;

    public Worker(NodeId id, List<NodeId> schedulers, Transport transport, EffectStore store,
                  Map<String, Handler> handlers) {
        this.id = id;
        this.schedulers = List.copyOf(schedulers);
        this.transport = transport;
        this.store = store;
        this.handlers = Map.copyOf(handlers);
    }

    public NodeId id() {
        return id;
    }

    /** Freeze this worker for the given number of ticks. */
    public void pause(int ticks) {
        pausedTicks = Math.max(pausedTicks, ticks);
    }

    public boolean isBusy() {
        return job != null;
    }

    public void tick() {
        tick++;
        if (pausedTicks > 0) {
            pausedTicks--;
            if (pausedTicks > 0) {
                return;
            }
            List<Delivery> waiting = new ArrayList<>(queued);
            queued.clear();
            for (Delivery d : waiting) {
                process(d.from(), d.message());
            }
        }
        if (tick - lastHeard > LEADER_SILENCE_TICKS) {
            leaderGuess = (leaderGuess + 1) % schedulers.size();
            lastHeard = tick;
        }
        if (job == null) {
            if (++sincePoll >= POLL_INTERVAL_TICKS) {
                sincePoll = 0;
                send(new WorkerMessages.Poll(id.value()));
            }
            return;
        }
        job.sinceHeartbeat++;
        if (job.sinceHeartbeat >= Math.max(1, job.leaseTicks / 3)) {
            job.sinceHeartbeat = 0;
            send(new WorkerMessages.Heartbeat(id.value(), job.runId, job.token));
        }
        if (job.phase == Phase.RUNNING) {
            if (--job.remaining <= 0) {
                finish();
            }
        } else if (++job.sinceReport >= REPORT_RETRY_TICKS) {
            sendCompleted();
        }
    }

    public void handle(NodeId from, Message message) {
        if (pausedTicks > 0) {
            queued.add(new Delivery(from, message));
            return;
        }
        process(from, message);
    }

    private void process(NodeId from, Message message) {
        if (schedulers.contains(from)) {
            lastHeard = tick;
        }
        switch (message) {
            case WorkerMessages.NotLeader n -> {
                int hint = n.leaderHint() == null ? -1 : schedulers.indexOf(n.leaderHint());
                leaderGuess = hint >= 0 ? hint : (leaderGuess + 1) % schedulers.size();
            }
            case WorkerMessages.Assignment a -> {
                if (a.workerId().equals(id.value()) && job == null) {
                    start(a);
                }
            }
            case WorkerMessages.HeartbeatAck ack -> {
                if (job != null && job.runId.equals(ack.runId()) && job.token == ack.token() && !ack.valid()) {
                    job = null; // the leader says this assignment is no longer current
                }
            }
            case WorkerMessages.CompletedAck ack -> {
                if (job != null && job.runId.equals(ack.runId()) && job.token == ack.token()) {
                    job = null;
                }
            }
            default -> { }
        }
    }

    private void start(WorkerMessages.Assignment a) {
        Job j = new Job();
        j.runId = a.runId();
        j.token = a.token();
        j.handlerName = a.handler();
        j.args = a.args();
        j.leaseTicks = a.leaseTicks();
        Handler h = handlers.get(a.handler());
        j.remaining = h == null ? 1 : h.durationTicks(a.args());
        job = j;
        send(new WorkerMessages.Started(id.value(), j.runId, j.token));
    }

    private void finish() {
        Handler h = handlers.get(job.handlerName);
        if (h == null) {
            job.success = false;
            job.detail = "no such handler: " + job.handlerName;
        } else {
            Handler.Result r = h.run(job.runId, job.token, job.args, store);
            if (r == Handler.Result.STALE) {
                job = null; // a newer assignment exists; reporting would only be rejected
                return;
            }
            job.success = r == Handler.Result.SUCCESS;
            job.detail = job.success ? "ok" : "handler failed";
        }
        job.phase = Phase.REPORTING;
        sendCompleted();
    }

    private void sendCompleted() {
        job.sinceReport = 0;
        send(new WorkerMessages.Completed(id.value(), job.runId, job.token, job.success, job.detail));
    }

    private void send(Message message) {
        transport.send(id, schedulers.get(leaderGuess), message);
    }
}
