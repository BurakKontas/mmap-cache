package tr.kontas.cache;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Represents a single on-disk data shard backed by a memory-mapped file.
 * Each shard stores fixed-size records and provides read/write/flush/close operations.
 */
@Slf4j
public class CacheShard implements AutoCloseable {
    @Getter
    private final String filePath;
    private final int recordSize;
    private final int capacity;
    private final int maxKeyBytes;
    private final int maxValueBytes;

    private FileChannel channel;
    private MappedByteBuffer buffer;
    @Getter
    private boolean closed = false;

    /**
     * Create a CacheShard given an absolute file path.
     *
     * @param filePath      file path for the shard
     * @param recordSize    size of each record in bytes
     * @param capacity      number of records the shard can hold
     * @param maxKeyBytes   max key bytes
     * @param maxValueBytes max value bytes
     */
    public CacheShard(String filePath, int recordSize, int capacity, int maxKeyBytes, int maxValueBytes) {
        this.filePath = filePath;
        this.recordSize = recordSize;
        this.capacity = capacity;
        this.maxKeyBytes = maxKeyBytes;
        this.maxValueBytes = maxValueBytes;
        try {
            initialize();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create cache shard: " + filePath, e);
        }
    }

    /**
     * Create a CacheShard from a Path.
     *
     * @param path         Path to the shard file
     * @param recordSize   size of each record in bytes
     * @param capacity     number of records the shard can hold
     * @param maxKeyBytes  max key bytes
     * @param maxValueBytes max value bytes
     */
    public CacheShard(Path path, int recordSize, int capacity, int maxKeyBytes, int maxValueBytes) {
        this(path.toString(), recordSize, capacity, maxKeyBytes, maxValueBytes);
    }

    /**
     * Creates a CacheShard from a File instance.
     *
     * @param file         File object for the shard
     * @param recordSize   size of each record in bytes
     * @param capacity     number of records the shard can hold
     * @param maxKeyBytes  max key bytes
     * @param maxValueBytes max value bytes
     */
    public CacheShard(File file, int recordSize, int capacity, int maxKeyBytes, int maxValueBytes) {
        this(file.getAbsolutePath(), recordSize, capacity, maxKeyBytes, maxValueBytes);
    }

    private static void unmap(MappedByteBuffer buf) {
        if (buf == null) return;
        try {
            var unsafeClass = Class.forName("sun.misc.Unsafe");
            var field = unsafeClass.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Object unsafe = field.get(null);
            unsafeClass.getMethod("invokeCleaner", ByteBuffer.class).invoke(unsafe, buf);
        } catch (Throwable t) {
            log.debug("Could not explicitly unmap buffer, leaving to GC", t);
        }
    }

    private void initialize() throws IOException {
        Path path = Paths.get(filePath);
        this.channel = FileChannel.open(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
        long fileSize = (long) recordSize * capacity;
        channel.truncate(fileSize);
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    }

    /**
     * Writes the provided entry into the shard at the given slot offset.
     *
     * @param offset slot index within the shard
     * @param entry  entry to write (reused by caller)
     * @return true if write succeeded; false when value exceeded maxValueBytes
     */
    public boolean write(int offset, CacheEntry entry) {
        if (closed) throw new IllegalStateException("Shard closed: " + filePath);
        int pos = offset * recordSize;
        assertBounds(pos, "write");
        buffer.position(pos);
        return entry.serialize(buffer);
    }

    /**
     * Reads an entry from the shard at the given offset.
     *
     * @param offset slot index
     * @return deserialized CacheEntry
     */
    public CacheEntry read(int offset) {
        if (closed) throw new IllegalStateException("Shard closed: " + filePath);
        int pos = offset * recordSize;
        assertBounds(pos, "read");
        buffer.position(pos);
        ByteBuffer slice = buffer.slice();
        slice.limit(recordSize);
        CacheEntry entry = new CacheEntry(maxKeyBytes, maxValueBytes);
        entry.deserialize(slice);
        return entry;
    }

    /**
     * Forces any in-memory changes to the underlying file.
     */
    public void flush() {
        if (closed) return;
        try {
            if (buffer != null) buffer.force();
        } catch (Exception e) {
            log.warn("Flush error on {}: {}", filePath, e.getMessage());
        }
    }

    /**
     * Closes this shard and releases resources.
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            flush();
            unmap(buffer);
            if (channel != null) channel.close();
        } catch (IOException e) {
            log.warn("Error closing cache shard {}", filePath, e);
        } finally {
            buffer = null;
        }
    }

    private void assertBounds(int pos, String op) {
        if (pos + recordSize > buffer.capacity())
            throw new IndexOutOfBoundsException(
                    "Shard " + op + " out of bounds: pos=" + pos +
                            ", recordSize=" + recordSize +
                            ", cap=" + buffer.capacity());
    }
}