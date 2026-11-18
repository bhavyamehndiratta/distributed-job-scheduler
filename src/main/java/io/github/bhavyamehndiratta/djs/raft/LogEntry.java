package io.github.bhavyamehndiratta.djs.raft;

/**
 * One entry in the replicated log. {@code term} is the term of the leader that
 * created it. A null {@code command} marks a no-op, which a new leader appends
 * to commit everything before it. Commands are opaque text to Raft; the
 * scheduler stage decides what they contain.
 */
public record LogEntry(long term, String command) {

    public static LogEntry noop(long term) {
        return new LogEntry(term, null);
    }

    public boolean isNoop() {
        return command == null;
    }
}
