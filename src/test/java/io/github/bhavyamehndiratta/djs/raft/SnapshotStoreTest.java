package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SnapshotStoreTest {

    private static final String NASTY = "a|b\\c\nd\u00e9\u4e2d\u0000x";

    @Test
    void inMemoryStoreKeepsTheLatest() {
        InMemorySnapshotStore s = new InMemorySnapshotStore();
        assertTrue(s.load() == null);
        s.save(new Snapshot(5, 2, "one"));
        s.save(new Snapshot(9, 3, "two"));
        assertEquals(new Snapshot(9, 3, "two"), s.load());
    }

    @Test
    void fileStoreRoundTripsAndOverwrites(@TempDir Path dir) {
        Path file = dir.resolve("snap");
        FileSnapshotStore s = new FileSnapshotStore(file);
        assertTrue(s.load() == null);
        s.save(new Snapshot(5, 2, NASTY));
        assertEquals(new Snapshot(5, 2, NASTY), new FileSnapshotStore(file).load());
        s.save(new Snapshot(9, 3, ""));
        assertEquals(new Snapshot(9, 3, ""), new FileSnapshotStore(file).load());
    }

    @Test
    void aCorruptOrTruncatedFileRefusesToLoad(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("snap");
        new FileSnapshotStore(file).save(new Snapshot(5, 2, "some state"));
        byte[] good = Files.readAllBytes(file);

        byte[] flipped = good.clone();
        flipped[flipped.length - 1] ^= 1;
        Files.write(file, flipped);
        assertThrows(IllegalStateException.class, () -> new FileSnapshotStore(file).load());

        Files.write(file, java.util.Arrays.copyOf(good, good.length - 3));
        assertThrows(IllegalStateException.class, () -> new FileSnapshotStore(file).load());

        byte[] wrongIndex = good.clone();
        wrongIndex[11] ^= 1;
        Files.write(file, wrongIndex);
        assertThrows(IllegalStateException.class, () -> new FileSnapshotStore(file).load());

        Files.write(file, good);
        assertNotNull(new FileSnapshotStore(file).load());
    }

    @Test
    void aLeftoverTempFileIsRemoved(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("snap");
        new FileSnapshotStore(file).save(new Snapshot(5, 2, "ok"));
        Files.write(dir.resolve("snap.tmp"), new byte[] {9, 9});
        FileSnapshotStore s = new FileSnapshotStore(file);
        assertEquals(new Snapshot(5, 2, "ok"), s.load());
        assertTrue(!Files.exists(dir.resolve("snap.tmp")));
    }
}
