package io.github.bhavyamehndiratta.djs.scheduler;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;

/** Messages between workers and the scheduler cluster. Workers pull; the leader never pushes unasked. */
public final class WorkerMessages {

    private WorkerMessages() {
    }

    /** An idle worker asks the leader for work. */
    public record Poll(String workerId) implements Message {
    }

    /** The leader's answer: run this, under this fencing token, and heartbeat within the lease. */
    public record Assignment(String workerId, String runId, long token, String handler, String args,
                             long leaseTicks) implements Message {
    }

    public record Heartbeat(String workerId, String runId, long token) implements Message {
    }

    /** {@code valid} is false when the token is no longer current, which tells the worker to abandon the run. */
    public record HeartbeatAck(String runId, long token, boolean valid) implements Message {
    }

    public record Started(String workerId, String runId, long token) implements Message {
    }

    public record Completed(String workerId, String runId, long token, boolean success,
                            String detail) implements Message {
    }

    /** {@code accepted} is false when the report carried a stale token. */
    public record CompletedAck(String runId, long token, boolean accepted) implements Message {
    }

    /** Sent by a node that is not the leader. {@code leaderHint} may be null. */
    public record NotLeader(NodeId leaderHint) implements Message {
    }
}
