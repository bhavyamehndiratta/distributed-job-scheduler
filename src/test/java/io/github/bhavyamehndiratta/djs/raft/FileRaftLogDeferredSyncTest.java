package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileRaftLogDeferredSyncTest {

    @Test
    void deferredAppendsAreVisibleAtOnceAndSurviveAReopenAfterSync(@TempDir Path dir) {
        Path file = dir.resolve("raft.log");
        try (FileRaftLog log = new FileRaftLog(file)) {
            log.setDeferredSync(true);
            log.append(new LogEntry(1, "a"));
            log.append(new LogEntry(1, "b"));
            assertEquals(2, log.lastIndex());
            log.sync();
            log.truncateFrom(2);
            log.append(new LogEntry(2, "c"));
            log.sync();
        }
        try (FileRaftLog again = new FileRaftLog(file)) {
            assertEquals(2, again.lastIndex());
            assertEquals("a", again.get(1).command());
            assertEquals("c", again.get(2).command());
            assertEquals(2, again.termAt(2));
        }
    }
}
