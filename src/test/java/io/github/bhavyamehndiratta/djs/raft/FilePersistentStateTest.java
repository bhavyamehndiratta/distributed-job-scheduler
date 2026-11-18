package io.github.bhavyamehndiratta.djs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FilePersistentStateTest {

    private static final NodeId N1 = new NodeId("n1");

    @TempDir
    Path dir;

    @Test
    void freshStateIsTermZeroWithNoVote() {
        FilePersistentState s = new FilePersistentState(dir.resolve("state"));
        assertEquals(0, s.currentTerm());
        assertNull(s.votedFor());
    }

    @Test
    void termAndVoteSurviveReopen() {
        Path f = dir.resolve("state");
        new FilePersistentState(f).setTermAndVote(7, N1);
        FilePersistentState again = new FilePersistentState(f);
        assertEquals(7, again.currentTerm());
        assertEquals(N1, again.votedFor());
    }

    @Test
    void aNewTermCanClearTheVote() {
        Path f = dir.resolve("state");
        FilePersistentState s = new FilePersistentState(f);
        s.setTermAndVote(3, N1);
        s.setTermAndVote(4, null);
        FilePersistentState again = new FilePersistentState(f);
        assertEquals(4, again.currentTerm());
        assertNull(again.votedFor());
    }

    @Test
    void termMayNotDecrease() {
        FilePersistentState s = new FilePersistentState(dir.resolve("state"));
        s.setTermAndVote(5, null);
        assertThrows(IllegalStateException.class, () -> s.setTermAndVote(4, null));
    }

    @Test
    void aFlippedByteMakesTheNodeRefuseToStart() throws IOException {
        Path f = dir.resolve("state");
        new FilePersistentState(f).setTermAndVote(7, N1);
        byte[] data = Files.readAllBytes(f);
        data[data.length - 1] ^= 0x01;
        Files.write(f, data);
        assertThrows(IllegalStateException.class, () -> new FilePersistentState(f));
    }

    @Test
    void aTruncatedFileMakesTheNodeRefuseToStart() throws IOException {
        Path f = dir.resolve("state");
        new FilePersistentState(f).setTermAndVote(7, N1);
        byte[] data = Files.readAllBytes(f);
        Files.write(f, java.util.Arrays.copyOf(data, 10));
        assertThrows(IllegalStateException.class, () -> new FilePersistentState(f));
    }

    @Test
    void anUnfinishedTempFileIsIgnoredAndRemoved() throws IOException {
        Path f = dir.resolve("state");
        new FilePersistentState(f).setTermAndVote(7, N1);
        Path temp = dir.resolve("state.tmp");
        Files.write(temp, new byte[] {1, 2, 3});
        FilePersistentState again = new FilePersistentState(f);
        assertEquals(7, again.currentTerm());
        assertEquals(N1, again.votedFor());
        assertFalse(Files.exists(temp));
    }

    @Test
    void noTempFileIsLeftBehindAfterAWrite() {
        Path f = dir.resolve("state");
        new FilePersistentState(f).setTermAndVote(2, N1);
        assertFalse(Files.exists(dir.resolve("state.tmp")));
    }
}
