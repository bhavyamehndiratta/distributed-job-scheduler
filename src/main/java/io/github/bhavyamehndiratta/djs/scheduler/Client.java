package io.github.bhavyamehndiratta.djs.scheduler;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.net.Transport;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Client library. Single threaded and driven by tick() and handle() like the
 * other actors, so the simulator can run it. It finds the leader by following
 * NotLeader hints, resends every request until it is answered, and gives up
 * after a timeout. Retrying is safe because a submit carries an idempotency
 * key: the cluster applies the key once, and a repeat gets DUPLICATE with the
 * original job id. A submit that times out may still have been committed, so
 * the caller can resubmit with the same key to find out.
 */
public final class Client {

    private static final int RETRY_TICKS = 15;
    private static final int SILENCE_TICKS = 40;

    /** A pending result. {@link #isDone} is true once answered or timed out. */
    public static final class Call<T> {
        private boolean done;
        private boolean timedOut;
        private T value;

        public boolean isDone() {
            return done;
        }

        public boolean isTimedOut() {
            return timedOut;
        }

        public T value() {
            if (!done || timedOut) {
                throw new IllegalStateException(timedOut ? "request timed out" : "not finished");
            }
            return value;
        }

        private void complete(T v) {
            value = v;
            done = true;
        }

        private void timeout() {
            timedOut = true;
            done = true;
        }
    }

    public record SubmitResult(ClientMessages.Status status, String jobId, String detail) {
    }

    public record CancelResult(ClientMessages.Status status, String detail) {
    }

    public record JobInfo(boolean found, JobView job, List<RunView> runs) {
    }

    public record MembersResult(ClientMessages.Status status, String detail, String voters, String learners) {
    }

    private static final class Request<T> {
        final long id;
        final Message message;
        final Call<T> call = new Call<>();
        final Function<Message, T> parse;
        int sinceSent = 0;
        int age = 0;
        int sends = 0;

        Request(long id, Message message, Function<Message, T> parse) {
            this.id = id;
            this.message = message;
            this.parse = parse;
        }
    }

    private final NodeId id;
    private final List<NodeId> schedulers;
    private final Transport transport;
    private final int timeoutTicks;
    private final Map<Long, Request<?>> inFlight = new LinkedHashMap<>();
    private int leaderGuess = 0;
    private long tick = 0;
    private long lastHeard = 0;
    private long nextRequestId = 1;
    private long nextKey = 1;

    public Client(NodeId id, List<NodeId> schedulers, Transport transport, int timeoutTicks) {
        this.id = id;
        this.schedulers = List.copyOf(schedulers);
        this.transport = transport;
        this.timeoutTicks = timeoutTicks;
    }

    public NodeId id() {
        return id;
    }

    /** Submits a job. A null key gets a fresh one, which still makes this call's own retries safe. */
    public Call<SubmitResult> submit(String jobId, JobSpec spec, String idempotencyKey) {
        String key = idempotencyKey != null ? idempotencyKey : id.value() + "-" + (nextKey++);
        long rid = nextRequestId++;
        return start(new Request<>(rid, new ClientMessages.Submit(rid, jobId, spec, key), m -> {
            ClientMessages.SubmitReply r = (ClientMessages.SubmitReply) m;
            return new SubmitResult(r.status(), r.jobId(), r.detail());
        }));
    }

    public Call<CancelResult> cancel(String jobId) {
        long rid = nextRequestId++;
        return start(new Request<>(rid, new ClientMessages.Cancel(rid, jobId), m -> {
            ClientMessages.CancelReply r = (ClientMessages.CancelReply) m;
            return new CancelResult(r.status(), r.detail());
        }));
    }

    public Call<JobInfo> getJob(String jobId) {
        long rid = nextRequestId++;
        return start(new Request<>(rid, new ClientMessages.GetJob(rid, jobId), m -> {
            ClientMessages.JobReply r = (ClientMessages.JobReply) m;
            return new JobInfo(r.found(), r.job(), r.runs());
        }));
    }

    public Call<List<JobView>> listJobs() {
        long rid = nextRequestId++;
        return start(new Request<>(rid, new ClientMessages.ListJobs(rid), m -> {
            ClientMessages.JobsReply r = (ClientMessages.JobsReply) m;
            return r.jobs();
        }));
    }

    /** Membership request: action is LIST, ADD, PROMOTE or REMOVE. See {@link ClientMessages.Members}. */
    public Call<MembersResult> members(String action, String nodeId, String address) {
        if (!List.of("LIST", "ADD", "PROMOTE", "REMOVE").contains(action)) {
            throw new IllegalArgumentException("unknown membership action: " + action);
        }
        long rid = nextRequestId++;
        return start(new Request<>(rid, new ClientMessages.Members(rid, action, nodeId, address), m -> {
            ClientMessages.MembersReply r = (ClientMessages.MembersReply) m;
            return new MembersResult(r.status(), r.detail(), r.voters(), r.learners());
        }));
    }

    private <T> Call<T> start(Request<T> request) {
        inFlight.put(request.id, request);
        send(request);
        return request.call;
    }

    public void tick() {
        tick++;
        if (inFlight.isEmpty()) {
            lastHeard = tick;
            return;
        }
        if (tick - lastHeard > SILENCE_TICKS) {
            leaderGuess = (leaderGuess + 1) % schedulers.size();
            lastHeard = tick;
        }
        Iterator<Request<?>> it = inFlight.values().iterator();
        while (it.hasNext()) {
            Request<?> r = it.next();
            r.age++;
            r.sinceSent++;
            if (r.age > timeoutTicks) {
                r.call.timeout();
                it.remove();
            } else if (r.sinceSent >= RETRY_TICKS) {
                send(r);
            }
        }
    }

    public void handle(NodeId from, Message message) {
        if (schedulers.contains(from)) {
            lastHeard = tick;
        }
        switch (message) {
            case WorkerMessages.NotLeader n -> {
                int hint = n.leaderHint() == null ? -1 : schedulers.indexOf(n.leaderHint());
                leaderGuess = hint >= 0 ? hint : (leaderGuess + 1) % schedulers.size();
                for (Request<?> r : new ArrayList<>(inFlight.values())) {
                    send(r);
                }
            }
            case ClientMessages.SubmitReply r -> finish(r.requestId(), r);
            case ClientMessages.CancelReply r -> finish(r.requestId(), r);
            case ClientMessages.JobReply r -> finish(r.requestId(), r);
            case ClientMessages.JobsReply r -> finish(r.requestId(), r);
            case ClientMessages.MembersReply r -> finish(r.requestId(), r);
            default -> { }
        }
    }

    private void finish(long requestId, Message reply) {
        Request<?> r = inFlight.remove(requestId);
        if (r != null) {
            // A retried request can see its own earlier copy as DUPLICATE, and over a real network the two
            // replies can arrive in either order. DUPLICATE on a retry means this call took effect.
            if (r.sends > 1 && reply instanceof ClientMessages.SubmitReply sr
                    && sr.status() == ClientMessages.Status.DUPLICATE
                    && r.message instanceof ClientMessages.Submit sent && sent.jobId().equals(sr.jobId())) {
                reply = new ClientMessages.SubmitReply(sr.requestId(), ClientMessages.Status.ACCEPTED, sr.jobId(),
                        "ACCEPTED (retried)");
            } else if (r.sends > 1 && reply instanceof ClientMessages.CancelReply cr
                    && cr.status() == ClientMessages.Status.DUPLICATE) {
                reply = new ClientMessages.CancelReply(cr.requestId(), ClientMessages.Status.ACCEPTED,
                        "ACCEPTED (retried)");
            } else if (r.sends > 1 && reply instanceof ClientMessages.MembersReply mr
                    && mr.status() == ClientMessages.Status.DUPLICATE) {
                // A change that committed while this request was being retried (or after the old leader
                // answered nobody) shows up as already in effect: that means it worked.
                reply = new ClientMessages.MembersReply(mr.requestId(), ClientMessages.Status.ACCEPTED,
                        "ACCEPTED (retried)", mr.voters(), mr.learners());
            }
            complete(r, reply);
        }
    }

    private static <T> void complete(Request<T> r, Message reply) {
        r.call.complete(r.parse.apply(reply));
    }

    private void send(Request<?> r) {
        r.sinceSent = 0;
        r.sends++;
        transport.send(id, schedulers.get(leaderGuess), r.message);
    }
}
