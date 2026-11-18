package io.github.bhavyamehndiratta.djs.scheduler;

/**
 * The payloads of the replicated log. Each is a pure description of a change;
 * the state machine decides whether it applies. Any time a command needs is
 * carried inside it, stamped by the leader that proposed it.
 */
public sealed interface Command {

    record SubmitJob(String jobId, JobSpec spec, String idempotencyKey, long now) implements Command {
        public SubmitJob {
            if (jobId == null || jobId.isBlank()) {
                throw new IllegalArgumentException("jobId must not be blank");
            }
            if (spec == null) {
                throw new IllegalArgumentException("spec must not be null");
            }
            if (idempotencyKey != null && idempotencyKey.isBlank()) {
                idempotencyKey = null;
            }
            if (now < 0) {
                throw new IllegalArgumentException("now must not be negative");
            }
        }
    }

    record CancelJob(String jobId) implements Command {
        public CancelJob {
            if (jobId == null || jobId.isBlank()) {
                throw new IllegalArgumentException("jobId must not be blank");
            }
        }
    }

    /** {@code skippedCount} is how many older missed fire times the leader skipped (misfire policy). */
    record FireRun(String jobId, long scheduledTime, long skippedCount) implements Command {
        public FireRun {
            if (jobId == null || jobId.isBlank()) {
                throw new IllegalArgumentException("jobId must not be blank");
            }
            if (scheduledTime < 0 || skippedCount < 0) {
                throw new IllegalArgumentException("scheduledTime and skippedCount must not be negative");
            }
        }
    }

    record AssignRun(String runId, String workerId, long leaseTicks) implements Command {
        public AssignRun {
            if (runId == null || runId.isBlank() || workerId == null || workerId.isBlank()) {
                throw new IllegalArgumentException("runId and workerId must not be blank");
            }
            if (leaseTicks < 1) {
                throw new IllegalArgumentException("leaseTicks must be at least 1");
            }
        }
    }

    record RunStarted(String runId, long token) implements Command {
        public RunStarted {
            if (runId == null || runId.isBlank() || token < 1) {
                throw new IllegalArgumentException("runId must not be blank and token must be positive");
            }
        }
    }

    record RunCompleted(String runId, long token, boolean success, String detail) implements Command {
        public RunCompleted {
            if (runId == null || runId.isBlank() || token < 1) {
                throw new IllegalArgumentException("runId must not be blank and token must be positive");
            }
            if (detail == null) {
                detail = "";
            }
        }
    }

    record ExpireAssignment(String runId, long token) implements Command {
        public ExpireAssignment {
            if (runId == null || runId.isBlank() || token < 1) {
                throw new IllegalArgumentException("runId must not be blank and token must be positive");
            }
        }
    }
}
