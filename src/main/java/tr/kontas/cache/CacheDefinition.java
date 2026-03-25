package tr.kontas.cache;


import lombok.Builder;
import lombok.Getter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Getter
@Builder(toBuilder = true)
public final class CacheDefinition<V> {
    public static final int DEFAULT_MAX_KEY_BYTES = 256;
    public static final int DEFAULT_MAX_VALUE_BYTES = 1024;

    private final String name;
    private final Supplier<Stream<CacheRow>> supplier;
    private final Function<CacheRow, String> keyExtractor;
    private final Function<CacheRow, byte[]> serializer;
    private final Function<byte[], V> deserializer;
    private final Duration ttl;

    @Builder.Default
    private final boolean dynamicSizing = false;

    @Builder.Default
    private final int maxKeyBytes = DEFAULT_MAX_KEY_BYTES;

    @Builder.Default
    private final int maxValueBytes = DEFAULT_MAX_VALUE_BYTES;

    public static Function<CacheRow, byte[]> defaultSerializer() {
        return row -> {
            String s = row.getValueAsString();
            return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
        };
    }

    public static <V> Function<byte[], V> defaultDeserializer(Function<String, V> stringParser) {
        return bytes -> {
            if (bytes == null || bytes.length == 0) return null;
            String s = new String(bytes, StandardCharsets.UTF_8);
            return stringParser.apply(s);
        };
    }

    public int recordSize() {
        // id(8) + keyLen(2) + key(maxKeyBytes) + valueLen(2) + value(maxValueBytes) + timestamp(8)
        return Long.BYTES + Short.BYTES + maxKeyBytes
                + Short.BYTES + maxValueBytes + Long.BYTES;
    }

    public int recordSize(int resolvedMaxKeyBytes, int resolvedMaxValueBytes) {
        return Long.BYTES + Short.BYTES + resolvedMaxKeyBytes
                + Short.BYTES + resolvedMaxValueBytes + Long.BYTES;
    }
}