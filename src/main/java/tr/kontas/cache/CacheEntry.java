package tr.kontas.cache;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Slf4j
@Getter
public final class CacheEntry {
    private final int maxKeyBytes;
    private final int maxValueBytes;

    private long id;
    private long timestamp;
    private String key;
    private byte[] valueBytes;

    public CacheEntry(int maxKeyBytes, int maxValueBytes) {
        this.maxKeyBytes = maxKeyBytes;
        this.maxValueBytes = maxValueBytes;
    }

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