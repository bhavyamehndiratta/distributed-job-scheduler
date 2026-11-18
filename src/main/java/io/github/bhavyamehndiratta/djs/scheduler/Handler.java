package io.github.bhavyamehndiratta.djs.scheduler;

/**
 * A registered job body. Jobs name a handler; they never carry code or shell
 * commands, because an effect that is not fenced cannot be made exactly-once.
 */
public interface Handler {

    enum Result { SUCCESS, FAILURE, STALE }

    /** How many ticks the job takes. */
    int durationTicks(String args);

    /** Performs the job's effect. STALE means a newer assignment exists, so the worker must drop the run. */
    Result run(String runId, long token, String args, EffectStore store);
}
