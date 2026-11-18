package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileRaftLogTest {

    @TempDir
    Path dir;

    private static void assertSameLog(RaftLog expected, RaftLog actual) {
        assertEquals(expected.lastIndex(), actual.lastIndex());
        for (long i = 1; i <= expected.lastIndex(); i++) {
            assertEquals(expected.get(i), actual.get(i), "entry " + i);
        }
    }

    @Test
    void entriesSurviveReopenIncludingNoopsEmptyAndUnicodeCommands() {
        Path f = dir.resolve("log");
        List<LogEntry> written = List.of(LogEntry.noop(1), new LogEntry(1, "submit job-1"),
                new LogEntry(2, ""), new LogEntry(3, "caf\u00e9 \u2713"), LogEntry.noop(4));
        try (FileRaftLog log = new FileRaftLog(f)) {
            written.forEach(log::append);
        }
        try (FileRaftLog again = new FileRaftLog(f)) {
            assertEquals(5, again.lastIndex());
            for (int i = 0; i < written.size(); i++) {
                assertEquals(written.get(i), again.get(i + 1));
            }
            assertEquals(4, again.lastTerm());
        }
    }

    @Test
    void truncationIsDurableAndAppendingAfterItWorks() {
        Path f = dir.resolve("log");
        try (FileRaftLog log = new FileRaftLog(f)) {
            for (int i = 1; i <= 5; i++) {
                log.append(new LogEntry(1, "c" + i));
            }
            log.truncateFrom(3);
            log.append(new LogEntry(2, "new3"));
        }
        try (FileRaftLog again = new FileRaftLog(f)) {
            assertEquals(3, again.lastIndex());
            assertEquals(new LogEntry(1, "c2"), again.get(2));
            assertEquals(new LogEntry(2, "new3"), again.get(3));
        }
    }

    @Test
    void aCrashAtAnyByteOfTheLastWriteRecoversToTheLastWholeRecord() throws IOException {
        Path f = dir.resolve("log");
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);
        try (FileRaftLog log = new FileRaftLog(f)) {
            for (int i = 1; i <= 3; i++) {
                log.append(new LogEntry(i, "command-" + i));
                boundaries.add(Files.size(f));
            }
        }
        byte[] full = Files.readAllBytes(f);
        for (int cut = 0; cut <= full.length; cut++) {
            Path copy = dir.resolve("cut-" + cut);
            Files.write(copy, Arrays.copyOf(full, cut));
            int whole = 0;
            for (int b = 1; b < boundaries.size(); b++) {
                if (boundaries.get(b) <= cut) {
                    whole = b;
                }
            }
            try (FileRaftLog log = new FileRaftLog(copy)) {
                assertEquals(whole, log.lastIndex(), "cut at " + cut);
                assertEquals(boundaries.get(whole).longValue(), Files.size(copy), "file repaired at cut " + cut);
                log.append(new LogEntry(9, "after-recovery"));
            }
            try (FileRaftLog again = new FileRaftLog(copy)) {
                assertEquals(whole + 1, again.lastIndex(), "cut at " + cut);
                assertEquals(new LogEntry(9, "after-recovery"), again.get(whole + 1));
            }
        }
    }

    @Test
    void zerosAppendedByACrashAreDropped() throws IOException {
        Path f = dir.resolve("log");
        try (FileRaftLog log = new FileRaftLog(f)) {
            log.append(new LogEntry(1, "a"));
            log.append(new LogEntry(1, "b"));
        }
        long good = Files.size(f);
        Files.write(f, new byte[20], java.nio.file.StandardOpenOption.APPEND);
        try (FileRaftLog again = new FileRaftLog(f)) {
            assertEquals(2, again.lastIndex());
        }
        assertEquals(good, Files.size(f));
    }

    @Test
    void aBadLastRecordIsTreatedAsATornWrite() throws IOException {
        Path f = dir.resolve("log");
        long firstEnd;
        try (FileRaftLog log = new FileRaftLog(f)) {
            log.append(new LogEntry(1, "a"));
            firstEnd = Files.size(f);
            log.append(new LogEntry(1, "b"));
        }
        byte[] data = Files.readAllBytes(f);
        data[data.length - 1] ^= 0x01;
        Files.write(f, data);
        try (FileRaftLog again = new FileRaftLog(f)) {
            assertEquals(1, again.lastIndex());
        }
        assertEquals(firstEnd, Files.size(f));
    }

    @Test
    void corruptionBeforeTheLastRecordMakesTheNodeRefuseToStart() throws IOException {
        Path f = dir.resolve("log");
        try (FileRaftLog log = new FileRaftLog(f)) {
            for (int i = 1; i <= 3; i++) {
                log.append(new LogEntry(1, "command-" + i));
            }
        }
        byte[] data = Files.readAllBytes(f);
        data[12] ^= 0x01; // inside the first record's payload
        Files.write(f, data);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new FileRaftLog(f));
        assertTrue(e.getMessage().contains("refusing to start"));
    }

    @Test
    void behavesLikeTheInMemoryLogUnderRandomOperationsAndReopens() {
        Random random = new Random(42);
        Path f = dir.resolve("log");
        InMemoryRaftLog expected = new InMemoryRaftLog();
        FileRaftLog actual = new FileRaftLog(f);
        long term = 1;
        for (int step = 0; step < 1500; step++) {
            int action = random.nextInt(10);
            if (action < 6 || expected.lastIndex() == 0) {
                if (random.nextInt(8) == 0) {
                    term++;
                }
                LogEntry entry = random.nextInt(10) == 0 ? LogEntry.noop(term) : new LogEntry(term, "c" + step);
                expected.append(entry);
                actual.append(entry);
            } else if (action < 8) {
                long index = 1 + random.nextInt((int) expected.lastIndex());
                expected.truncateFrom(index);
                actual.truncateFrom(index);
            } else {
                actual.close();
                actual = new FileRaftLog(f);
            }
            assertEquals(expected.lastIndex(), actual.lastIndex());
        }
        assertSameLog(expected, actual);
        actual.close();
        try (FileRaftLog reopened = new FileRaftLog(f)) {
            assertSameLog(expected, reopened);
        }
    }
}
