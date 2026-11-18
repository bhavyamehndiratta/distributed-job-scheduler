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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Append-only log file. Each record is: payload length (int), crc32 of the
 * payload (int), payload. The payload is the term (long), a flag byte (0 for a
 * no-op, 1 for a command, 2 for a base record) and then the command as UTF-8,
 * or for a base record the snapshot index (long). A base record can only be the
 * first record: it says the file starts after a snapshot at that index and
 * term. {@link #append} and {@link #truncateFrom} fsync before returning.
 *
 * <p>Compaction rewrites the file: the new content goes to a temp file, is
 * fsynced, and replaces the old file with an atomic rename, so a crash leaves
 * either the old log or the new one. Because the snapshot is saved before the
 * log is compacted, a leftover old log is harmless: the node compacts it again
 * on start.
 *
 * <p>Recovery on open: a record cut short by a crash at the end of the file is
 * a torn write, so it is dropped and the file is trimmed. A last record whose
 * checksum fails is treated the same way (it cannot be told apart from a torn
 * write). A bad record with more data after it is real corruption and the node
 * refuses to start rather than guess. If a write fails with an IOException the
 * node must stop; the in-memory view is no longer trustworthy.
 */
public final class FileRaftLog implements RaftLog, AutoCloseable {

    private static final int HEADER = 8;
    private static final int MIN_PAYLOAD = 9;
    private static final int BASE_PAYLOAD = MIN_PAYLOAD + 8;
    private static final int MAX_PAYLOAD = 64 * 1024 * 1024;
    private static final byte FLAG_NOOP = 0;
    private static final byte FLAG_COMMAND = 1;
    private static final byte FLAG_BASE = 2;

    private final Path file;
    private final Path temp;
    private FileChannel channel;
    private final List<LogEntry> entries = new ArrayList<>();
    private final List<Long> starts = new ArrayList<>();
    private long baseIndex = 0;
    private long baseTerm = 0;
    private long end = 0;
    private boolean deferSync = false;
    private boolean dirty = false;

    public FileRaftLog(Path file) {
        this.file = file;
        this.temp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            Files.deleteIfExists(temp);
            this.channel = FileChannel.open(file, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            recover();
        } catch (IOException | RuntimeException e) {
            close();
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new UncheckedIOException((IOException) e);
        }
    }

    @Override
    public long lastIndex() {
        return baseIndex + entries.size();
    }

    @Override
    public long snapshotIndex() {
        return baseIndex;
    }

    @Override
    public long snapshotTerm() {
        return baseTerm;
    }

    @Override
    public long termAt(long index) {
        if (index == baseIndex) {
            return baseTerm;
        }
        return get(index).term();
    }

    @Override
    public LogEntry get(long index) {
        if (index <= baseIndex || index > lastIndex()) {
            throw new IllegalArgumentException("no entry at index " + index + ", log covers ("
                    + baseIndex + ", " + lastIndex() + "]");
        }
        return entries.get((int) (index - baseIndex - 1));
    }

    @Override
    public void append(LogEntry entry) {
        try {
            long length = writeRecord(channel, end, encode(entry));
            markWritten();
            starts.add(end);
            entries.add(entry);
            end += length;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void truncateFrom(long index) {
        if (index <= baseIndex || index > lastIndex()) {
            throw new IllegalArgumentException("cannot truncate from " + index + ", log covers ("
                    + baseIndex + ", " + lastIndex() + "]");
        }
        int pos = (int) (index - baseIndex - 1);
        long newEnd = starts.get(pos);
        try {
            channel.truncate(newEnd);
            markWritten();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        entries.subList(pos, entries.size()).clear();
        starts.subList(pos, starts.size()).clear();
        end = newEnd;
    }

    @Override
    public void compactTo(long index, long term) {
        if (index <= baseIndex) {
            throw new IllegalArgumentException("cannot compact to " + index + ", already compacted to " + baseIndex);
        }
        if (index <= lastIndex() && termAt(index) != term) {
            throw new IllegalArgumentException("term mismatch at " + index);
        }
        List<LogEntry> keep = index >= lastIndex()
                ? List.of()
                : new ArrayList<>(entries.subList((int) (index - baseIndex), entries.size()));
        rewrite(index, term, keep);
    }

    @Override
    public void reset(long index, long term) {
        if (index < 0) {
            throw new IllegalArgumentException("negative index");
        }
        rewrite(index, term, List.of());
    }

    /**
     * With deferred sync, {@link #append} and {@link #truncateFrom} write without fsync and the caller must
     * call {@link #sync()} before acting on them (group commit: one fsync for a whole batch of entries).
     */
    public void setDeferredSync(boolean deferred) {
        if (!deferred) {
            sync();
        }
        this.deferSync = deferred;
    }

    @Override
    public void sync() {
        if (!dirty) {
            return;
        }
        try {
            channel.force(true);
            dirty = false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void markWritten() throws IOException {
        if (deferSync) {
            dirty = true;
        } else {
            channel.force(true);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Writes the new log to a temp file and swaps it in atomically. */
    private void rewrite(long newBaseIndex, long newBaseTerm, List<LogEntry> retained) {
        List<Long> newStarts = new ArrayList<>();
        try {
            long pos = 0;
            try (FileChannel t = FileChannel.open(temp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                if (newBaseIndex > 0) {
                    pos += writeRecord(t, pos, encodeBase(newBaseIndex, newBaseTerm));
                }
                for (LogEntry e : retained) {
                    newStarts.add(pos);
                    pos += writeRecord(t, pos, encode(e));
                }
                t.force(true);
            }
            channel.close();
            Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
            try (FileChannel dir = FileChannel.open(file.toAbsolutePath().getParent(), StandardOpenOption.READ)) {
                dir.force(true);
            }
            channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            entries.clear();
            entries.addAll(retained);
            starts.clear();
            starts.addAll(newStarts);
            baseIndex = newBaseIndex;
            baseTerm = newBaseTerm;
            end = pos;
            dirty = false; // the new file was fully fsynced
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long writeRecord(FileChannel ch, long pos, byte[] payload) throws IOException {
        CRC32 crc = new CRC32();
        crc.update(payload);
        ByteBuffer record = ByteBuffer.allocate(HEADER + payload.length);
        record.putInt(payload.length);
        record.putInt((int) crc.getValue());
        record.put(payload);
        record.flip();
        long written = 0;
        while (record.hasRemaining()) {
            written += ch.write(record, pos + written);
        }
        return written;
    }

    private static byte[] encode(LogEntry entry) {
        byte[] command = entry.isNoop() ? new byte[0] : entry.command().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(MIN_PAYLOAD + command.length);
        buf.putLong(entry.term());
        buf.put(entry.isNoop() ? FLAG_NOOP : FLAG_COMMAND);
        buf.put(command);
        return buf.array();
    }

    private static byte[] encodeBase(long index, long term) {
        ByteBuffer buf = ByteBuffer.allocate(BASE_PAYLOAD);
        buf.putLong(term);
        buf.put(FLAG_BASE);
        buf.putLong(index);
        return buf.array();
    }

    private void recover() throws IOException {
        long size = channel.size();
        if (size > Integer.MAX_VALUE) {
            throw new IllegalStateException("log file too large: " + size);
        }
        ByteBuffer all = ByteBuffer.allocate((int) size);
        long pos = 0;
        while (all.hasRemaining()) {
            int n = channel.read(all, pos);
            if (n < 0) {
                break;
            }
            pos += n;
        }
        byte[] data = all.array();
        int off = 0;
        while (off < size) {
            if (size - off < HEADER) {
                break; // header cut short: torn write
            }
            int len = ByteBuffer.wrap(data, off, 4).getInt();
            int stored = ByteBuffer.wrap(data, off + 4, 4).getInt();
            if (len < MIN_PAYLOAD || len > MAX_PAYLOAD) {
                if (allZero(data, off, (int) size)) {
                    break; // file extended with zeros by a crash
                }
                throw corrupt(off, "bad record length " + len);
            }
            if (size - off - HEADER < len) {
                break; // payload cut short: torn write
            }
            CRC32 crc = new CRC32();
            crc.update(data, off + HEADER, len);
            if ((int) crc.getValue() != stored) {
                if (off + HEADER + len == size) {
                    break; // last record, indistinguishable from a torn write
                }
                throw corrupt(off, "checksum mismatch");
            }
            ByteBuffer p = ByteBuffer.wrap(data, off + HEADER, len);
            long term = p.getLong();
            byte flag = p.get();
            if (flag == FLAG_BASE) {
                if (off != 0 || len != BASE_PAYLOAD) {
                    throw corrupt(off, "misplaced base record");
                }
                baseTerm = term;
                baseIndex = p.getLong();
            } else if (flag == FLAG_NOOP && len == MIN_PAYLOAD) {
                entries.add(LogEntry.noop(term));
                starts.add((long) off);
            } else if (flag == FLAG_COMMAND) {
                entries.add(new LogEntry(term,
                        new String(data, off + HEADER + MIN_PAYLOAD, len - MIN_PAYLOAD, StandardCharsets.UTF_8)));
                starts.add((long) off);
            } else {
                throw corrupt(off, "bad entry flag " + flag);
            }
            off += HEADER + len;
        }
        if (off < size) {
            channel.truncate(off);
            channel.force(true);
        }
        end = off;
    }

    private static boolean allZero(byte[] data, int from, int to) {
        for (int i = from; i < to; i++) {
            if (data[i] != 0) {
                return false;
            }
        }
        return true;
    }

    private IllegalStateException corrupt(int offset, String why) {
        return new IllegalStateException("corrupt log " + file + " at byte " + offset + ": " + why
                + ", refusing to start");
    }
}
