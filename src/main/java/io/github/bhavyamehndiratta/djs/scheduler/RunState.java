package io.github.bhavyamehndiratta.djs.scheduler;

public enum RunState {
    SCHEDULED,
    ASSIGNED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
