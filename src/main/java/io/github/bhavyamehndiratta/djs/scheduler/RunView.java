package io.github.bhavyamehndiratta.djs.scheduler;

/**
 * Read-only snapshot of a run. {@code token} is the fencing token of the
 * current assignment (the log index of its AssignRun entry), or 0 when the run
 * has no current assignment.
 */
public record RunView(String runId, String jobId, long scheduledTime, RunState state, int attempts,
                      long token, String workerId, long leaseTicks, String detail) {
}
