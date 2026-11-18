package io.github.bhavyamehndiratta.djs.raft;

/** State machine state as of log index {@code index}, whose entry had term {@code term}. */
public record Snapshot(long index, long term, String data) {
    public Snapshot {
        if (index < 1 || term < 0 || data == null) {
            throw new IllegalArgumentException("bad snapshot");
        }
    }
}
