package io.github.bhavyamehndiratta.djs.raft;

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
 * One snapshot file, replaced atomically (temp file, fsync, rename, directory
 * fsync), so a crash leaves either the old snapshot or the new one, never a
 * mix. Layout: magic, index, term, data length, crc32 of index/term/length/data,
 * then the data as UTF-8. A file that fails its checks is never guessed at:
 * the node refuses to start.
 */
public final class FileSnapshotStore implements SnapshotStore {

    private static final int MAGIC = 0x534E4150;
    private static final int HEADER = 28;

    private final Path file;
    private final Path temp;

    public FileSnapshotStore(Path file) {
        this.file = file;
        this.temp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            Files.deleteIfExists(temp);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Snapshot load() {
        if (!Files.exists(file)) {
            return null;
        }
        byte[] all;
        try {
            all = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (all.length < HEADER) {
            throw corrupt("file too short");
        }
        ByteBuffer b = ByteBuffer.wrap(all);
        if (b.getInt() != MAGIC) {
            throw corrupt("bad magic");
        }
        long index = b.getLong();
        long term = b.getLong();
        int length = b.getInt();
        int stored = b.getInt();
        if (length < 0 || length != all.length - HEADER) {
            throw corrupt("bad length");
        }
        if (checksum(all, length) != stored) {
            throw corrupt("checksum mismatch");
        }
        return new Snapshot(index, term, new String(all, HEADER, length, StandardCharsets.UTF_8));
    }

    @Override
    public void save(Snapshot snapshot) {
        byte[] data = snapshot.data().getBytes(StandardCharsets.UTF_8);
        ByteBuffer b = ByteBuffer.allocate(HEADER + data.length);
        b.putInt(MAGIC);
        b.putLong(snapshot.index());
        b.putLong(snapshot.term());
        b.putInt(data.length);
        b.putInt(0);
        b.put(data);
        byte[] all = b.array();
        ByteBuffer.wrap(all).putInt(24, checksum(all, data.length));
        try {
            try (FileChannel ch = FileChannel.open(temp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                ByteBuffer out = ByteBuffer.wrap(all);
                while (out.hasRemaining()) {
                    ch.write(out);
                }
                ch.force(true);
            }
            Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
            try (FileChannel dir = FileChannel.open(file.toAbsolutePath().getParent(), StandardOpenOption.READ)) {
                dir.force(true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Covers index, term and length (bytes 4 to 24) and the data. */
    private static int checksum(byte[] all, int dataLength) {
        CRC32 crc = new CRC32();
        crc.update(all, 4, 20);
        crc.update(all, HEADER, dataLength);
        return (int) crc.getValue();
    }

    private IllegalStateException corrupt(String why) {
        return new IllegalStateException("corrupt snapshot " + file + ": " + why + ", refusing to start");
    }
}
