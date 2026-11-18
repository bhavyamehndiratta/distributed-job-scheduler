package io.github.bhavyamehndiratta.djs.scheduler;

/** What the state machine did with a committed command. Every node reaches the same outcome. */
public enum Outcome {
    /** The command changed state. */
    APPLIED,
    /** Already done earlier (same idempotency key, same job id, repeated completion, old fire time). Nothing changed. */
    DUPLICATE,
    /** The job or run does not exist. */
    UNKNOWN,
    /** The report carries a token that is not the run's current token. Nothing changed. */
    STALE_TOKEN,
    /** Valid command, but not allowed in the current state. */
    REJECTED,
    /** The log entry could not be decoded. */
    MALFORMED
}
