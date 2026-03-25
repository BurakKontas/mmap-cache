package tr.kontas.cache;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Represents a fixed-size cache record used inside a shard file.
 * <p>
 * Instances are reused during writes to avoid per-row allocations; callers
 * should not retain references to these objects after writing to a shard.
 */
@Slf4j
@Getter
public final class CacheEntry {
    private final int maxKeyBytes;
    private final int maxValueBytes;

    private long id;
    private long timestamp;
    private String key;
    private byte[] valueBytes;

    /**
     * Creates a new CacheEntry helper used for reads/writes.
     *
     * @param maxKeyBytes   maximum key bytes for this entry
     * @param maxValueBytes maximum value bytes for this entry
     */
    public CacheEntry(int maxKeyBytes, int maxValueBytes) {
        this.maxKeyBytes = maxKeyBytes;
        this.maxValueBytes = maxValueBytes;
    }

    /**
     * Full constructor used by tests to create independent entry instances.
     *
     * @param id            entry id
     * @param timestamp     entry timestamp
     * @param key           entry key
     * @param valueBytes    entry bytes
     * @param maxKeyBytes   max key bytes
     * @param maxValueBytes max value bytes
     */
    public CacheEntry(long id, long timestamp, String key, byte[] valueBytes,
                      int maxKeyBytes, int maxValueBytes) {
        this.id = id;
        this.timestamp = timestamp;
        this.key = key;
        this.valueBytes = valueBytes;
        this.maxKeyBytes = maxKeyBytes;
        this.maxValueBytes = maxValueBytes;
    }

    private static void skipBytes(ByteBuffer buf, int count) {
        if (count > 0) buf.position(buf.position() + count);
    }

    public void reset(long id, long timestamp, String key, byte[] valueBytes) {
        this.id = id;
        this.timestamp = timestamp;
        this.key = key;
        this.valueBytes = valueBytes;
    }

    /**
     * Serializes this cache entry to the given ByteBuffer.
     *
     * @param buf target buffer
     * @return true if the entry was successfully serialized, false if the value
     * bytes were too large to fit
     */
    public boolean serialize(ByteBuffer buf) {
        buf.putLong(id);

        byte[] kb = key.getBytes(StandardCharsets.UTF_8);
        if (kb.length > maxKeyBytes)
            throw new IllegalArgumentException("Key too long: " + key);
        buf.putShort((short) kb.length);
        buf.put(kb);
        // padding
        skipBytes(buf, maxKeyBytes - kb.length);

        if (valueBytes.length > maxValueBytes) {
            log.warn("Cache value too large for key '{}': {} bytes (max {}), skipping write.",
                    key, valueBytes.length, maxValueBytes);
            return false;
        }
        buf.putShort((short) valueBytes.length);
        buf.put(valueBytes);
        // padding
        skipBytes(buf, maxValueBytes - valueBytes.length);

        buf.putLong(timestamp);
        return true;
    }

    /**
     * Deserializes this cache entry from the given ByteBuffer.
     *
     * @param buf source buffer
     */
    public void deserialize(ByteBuffer buf) {
        this.id = buf.getLong();

        int kLen = Short.toUnsignedInt(buf.getShort());
        byte[] kb = new byte[kLen];
        buf.get(kb);
        skipBytes(buf, maxKeyBytes - kLen);
        this.key = new String(kb, StandardCharsets.UTF_8);

        int vLen = Short.toUnsignedInt(buf.getShort());
        this.valueBytes = new byte[vLen];
        buf.get(this.valueBytes);
        skipBytes(buf, maxValueBytes - vLen);

        this.timestamp = buf.getLong();
    }
}