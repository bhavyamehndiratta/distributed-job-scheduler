package io.github.bhavyamehndiratta.djs.scheduler;

import io.github.bhavyamehndiratta.djs.net.Message;
import java.util.List;

/** Messages between clients and the scheduler cluster. A non-leader answers any of them with WorkerMessages.NotLeader. */
public final class ClientMessages {

    private ClientMessages() {
    }

    /** ACCEPTED: committed now. DUPLICATE: the idempotency key was seen before, nothing changed. REJECTED: not allowed. */
    public enum Status { ACCEPTED, DUPLICATE, REJECTED }

    public record Submit(long requestId, String jobId, JobSpec spec, String idempotencyKey) implements Message {
    }

    /** {@code jobId} is the job the idempotency key maps to, which may differ from the id asked for on a DUPLICATE. */
    public record SubmitReply(long requestId, Status status, String jobId, String detail) implements Message {
    }

    public record Cancel(long requestId, String jobId) implements Message {
    }

    public record CancelReply(long requestId, Status status, String detail) implements Message {
    }

    public record GetJob(long requestId, String jobId) implements Message {
    }

    public record ListJobs(long requestId) implements Message {
    }

    public record JobsReply(long requestId, List<JobView> jobs) implements Message {
    }

    /**
     * Membership request. {@code action} is LIST, ADD (needs nodeId and address), PROMOTE or REMOVE. A reply
     * with ACCEPTED means the change committed, DUPLICATE that it was already in effect, REJECTED that it
     * was refused for now (retry) or is invalid.
     */
    public record Members(long requestId, String action, String nodeId, String address) implements Message {
    }

    /** {@code voters} and {@code learners} are comma separated node ids as the leader sees them. */
    public record MembersReply(long requestId, Status status, String detail, String voters, String learners)
            implements Message {
    }

    /** Read from the leader's applied state. Stage 11 (ReadIndex) makes this linearizable. */
    public record JobReply(long requestId, boolean found, JobView job, List<RunView> runs) implements Message {
    }
}
