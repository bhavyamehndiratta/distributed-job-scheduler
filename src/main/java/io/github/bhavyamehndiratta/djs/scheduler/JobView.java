package io.github.bhavyamehndiratta.djs.scheduler;

/** Read-only snapshot of a job. {@code lastFiredTime} is -1 until the job first fires. */
public record JobView(String jobId, JobSpec spec, JobStatus status, long createdAt, long lastFiredTime,
                      long skippedCount, String idempotencyKey) {
}
