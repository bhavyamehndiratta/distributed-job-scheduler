package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftLogCompactionTest {

    private static LogEntry e(long term, String command) {
        return new LogEntry(term, command);
    }

    private static void fill(RaftLog log) {
        long[] terms = {1, 1, 1, 2, 2, 3, 3, 3, 4, 4};
        for (int i = 0; i < terms.length; i++) {
            log.append(e(terms[i], "c" + (i + 1)));
        }
    }

    private static void semantics(RaftLog log) {
        fill(log);
        log.compactTo(4, 2);
        assertEquals(4, log.snapshotIndex());
        assertEquals(2, log.snapshotTerm());
        assertEquals(10, log.lastIndex());
        assertEquals(2, log.termAt(4));
        assertEquals(2, log.termAt(5));
        assertEquals("c5", log.get(5).command());
        assertEquals("c10", log.get(10).command());
        assertThrows(IllegalArgumentException.class, () -> log.get(4));
        assertThrows(IllegalArgumentException.class, () -> log.termAt(3));
        assertThrows(IllegalArgumentException.class, () -> log.truncateFrom(4));
        assertThrows(IllegalArgumentException.class, () -> log.compactTo(4, 2));
        assertThrows(IllegalArgumentException.class, () -> log.compactTo(7, 99));

        log.truncateFrom(8);
        assertEquals(7, log.lastIndex());
        log.append(e(5, "x"));
        assertEquals(8, log.lastIndex());
        assertEquals("x", log.get(8).command());
        assertEquals(5, log.lastTerm());

        log.compactTo(12, 6); // beyond the end: everything goes
        assertEquals(12, log.snapshotIndex());
        assertEquals(12, log.lastIndex());
        assertEquals(6, log.lastTerm());
        log.append(e(6, "after"));
        assertEquals(13, log.lastIndex());
        assertEquals("after", log.get(13).command());

        log.reset(3, 1);
        assertEquals(3, log.lastIndex());
        assertEquals(3, log.snapshotIndex());
        assertEquals(1, log.termAt(3));
        log.reset(0, 0);
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.termAt(0));
    }

    @Test
    void inMemoryLogCompactionSemantics() {
        semantics(new InMemoryRaftLog());
    }

    @Test
    void fileLogCompactionSemantics(@TempDir Path dir) {
        try (FileRaftLog log = new FileRaftLog(dir.resolve("log"))) {
            semantics(log);
        }
    }

    @Test
    void aCompactedFileLogSurvivesReopening(@TempDir Path dir) {
        Path file = dir.resolve("log");
        try (FileRaftLog log = new FileRaftLog(file)) {
            fill(log);
            log.compactTo(6, 3);
            log.append(e(5, "tail"));
        }
        try (FileRaftLog log = new FileRaftLog(file)) {
            assertEquals(6, log.snapshotIndex());
            assertEquals(3, log.snapshotTerm());
            assertEquals(11, log.lastIndex());
            assertEquals("c7", log.get(7).command());
            assertEquals("tail", log.get(11).command());
            log.compactTo(11, 5);
        }
        try (FileRaftLog log = new FileRaftLog(file)) {
            assertEquals(11, log.snapshotIndex());
            assertEquals(11, log.lastIndex());
            assertEquals(5, log.termAt(11));
        }
    }

    @Test
    void aLeftoverTempFileFromACrashedCompactionIsIgnored(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("log");
        try (FileRaftLog log = new FileRaftLog(file)) {
            fill(log);
        }
        Files.write(dir.resolve("log.tmp"), new byte[] {1, 2, 3});
        try (FileRaftLog log = new FileRaftLog(file)) {
            assertEquals(10, log.lastIndex());
            assertEquals(0, log.snapshotIndex());
        }
    }

    @Test
    void aTornTailAfterACompactedFileIsDropped(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("log");
        try (FileRaftLog log = new FileRaftLog(file)) {
            fill(log);
            log.compactTo(5, 2);
        }
        long size = Files.size(file);
        Files.write(file, new byte[] {0, 0, 0, 40, 1, 2}, java.nio.file.StandardOpenOption.APPEND);
        try (FileRaftLog log = new FileRaftLog(file)) {
            assertEquals(5, log.snapshotIndex());
            assertEquals(10, log.lastIndex());
        }
        assertEquals(size, Files.size(file));
    }

    @Test
    void fileAndMemoryLogsAgreeUnderRandomOperationsWithReopens(@TempDir Path dir) {
        for (long seed = 1; seed <= 60; seed++) {
            Random rnd = new Random(seed);
            Path file = dir.resolve("log-" + seed);
            InMemoryRaftLog mem = new InMemoryRaftLog();
            FileRaftLog disk = new FileRaftLog(file);
            long term = 1;
            for (int step = 0; step < 150; step++) {
                int op = rnd.nextInt(100);
                long base = mem.snapshotIndex();
                long last = mem.lastIndex();
                if (op < 60) {
                    if (rnd.nextInt(8) == 0) {
                        term++;
                    }
                    LogEntry entry = rnd.nextInt(10) == 0 ? LogEntry.noop(term) : e(term, "s" + seed + "-" + step);
                    mem.append(entry);
                    disk.append(entry);
                } else if (op < 72) {
                    if (last > base) {
                        long from = base + 1 + rnd.nextInt((int) (last - base));
                        mem.truncateFrom(from);
                        disk.truncateFrom(from);
                    }
                } else if (op < 95) {
                    long index = base + 1 + rnd.nextInt((int) (last - base) + 2);
                    long t = index <= last ? mem.termAt(index) : Math.max(term, mem.lastTerm());
                    mem.compactTo(index, t);
                    disk.compactTo(index, t);
                } else {
                    long index = rnd.nextInt((int) last + 3);
                    mem.reset(index, index == 0 ? 0 : term);
                    disk.reset(index, index == 0 ? 0 : term);
                }
                if (step % 20 == 19) {
                    disk.close();
                    disk = new FileRaftLog(file);
                }
                assertSame(mem, disk, "seed " + seed + " step " + step);
            }
            disk.close();
        }
    }

    private static void assertSame(RaftLog a, RaftLog b, String where) {
        assertEquals(a.lastIndex(), b.lastIndex(), where);
        assertEquals(a.snapshotIndex(), b.snapshotIndex(), where);
        assertEquals(a.snapshotTerm(), b.snapshotTerm(), where);
        for (long i = a.snapshotIndex(); i <= a.lastIndex(); i++) {
            assertEquals(a.termAt(i), b.termAt(i), where + " term at " + i);
            if (i > a.snapshotIndex()) {
                assertEquals(a.get(i), b.get(i), where + " entry " + i);
            }
        }
    }
}
