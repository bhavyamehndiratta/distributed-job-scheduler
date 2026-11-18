package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * currentTerm and votedFor on disk. Each change is written to a temporary file,
 * fsynced, then atomically renamed over the real file, and the directory is
 * fsynced. A crash therefore leaves either the old state or the new state,
 * never a mix. A file whose checksum fails is not guessed at: the node refuses
 * to start, because a wrong vote record could allow two leaders in one term.
 *
 * <p>File layout: crc32 (int) over the rest, term (long), voted-for length
 * (int, -1 for none), voted-for bytes (UTF-8).
 */
public final class FilePersistentState implements PersistentState {

    private final Path file;
    private final Path temp;
    private long currentTerm = 0;
    private NodeId votedFor = null;

    public FilePersistentState(Path file) {
        this.file = file;
        this.temp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            Files.deleteIfExists(temp); // a leftover temp file is an unfinished write, safe to drop
            if (Files.exists(file)) {
                load();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public NodeId votedFor() {
        return votedFor;
    }

    @Override
    public void setTermAndVote(long term, NodeId votedFor) {
        if (term < currentTerm) {
            throw new IllegalStateException("term must never decrease: " + currentTerm + " -> " + term);
        }
        try {
            store(term, votedFor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.currentTerm = term;
        this.votedFor = votedFor;
    }

    private void store(long term, NodeId vote) throws IOException {
        byte[] name = vote == null ? new byte[0] : vote.value().getBytes(StandardCharsets.UTF_8);
        ByteBuffer body = ByteBuffer.allocate(8 + 4 + name.length);
        body.putLong(term);
        body.putInt(vote == null ? -1 : name.length);
        body.put(name);
        CRC32 crc = new CRC32();
        crc.update(body.array());
        ByteBuffer out = ByteBuffer.allocate(4 + body.capacity());
        out.putInt((int) crc.getValue());
        out.put(body.array());
        out.flip();

        try (FileChannel ch = FileChannel.open(temp, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            while (out.hasRemaining()) {
                ch.write(out);
            }
            ch.force(true);
        }
        Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
        try (FileChannel dir = FileChannel.open(file.toAbsolutePath().getParent(), StandardOpenOption.READ)) {
            dir.force(true);
        }
    }

    private void load() throws IOException {
        byte[] data = Files.readAllBytes(file);
        if (data.length < 16) {
            throw new IllegalStateException("corrupt state file " + file + ": too short, refusing to start");
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        int stored = buf.getInt();
        CRC32 crc = new CRC32();
        crc.update(data, 4, data.length - 4);
        if ((int) crc.getValue() != stored) {
            throw new IllegalStateException("corrupt state file " + file + ": checksum mismatch, refusing to start");
        }
        long term = buf.getLong();
        int nameLength = buf.getInt();
        if (nameLength < -1 || nameLength > buf.remaining() || (nameLength == -1 && buf.remaining() != 0)
                || (nameLength >= 0 && buf.remaining() != nameLength)) {
            throw new IllegalStateException("corrupt state file " + file + ": bad length, refusing to start");
        }
        NodeId vote = null;
        if (nameLength >= 0) {
            byte[] name = new byte[nameLength];
            buf.get(name);
            vote = new NodeId(new String(name, StandardCharsets.UTF_8));
        }
        this.currentTerm = term;
        this.votedFor = vote;
    }
}
