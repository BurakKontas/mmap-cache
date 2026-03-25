package tr.kontas.cache;

import lombok.Builder;
import lombok.Getter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Function;

/**
 * Declarative configuration for one cache namespace.
 * <p>
 * A definition contains data source, key/value mapping functions,
 * sizing preferences, and optional in-memory cache policies.
 *
 * @param <V> deserialized value type returned by the cache
 */
@Getter
@Builder(toBuilder = true)
public final class CacheDefinition<V> {
    public static final int DEFAULT_MAX_KEY_BYTES = 256;
    public static final int DEFAULT_MAX_VALUE_BYTES = 1024;

    // ── Basic Definition ───────────────────────────────────────────────────
    private final String name;

    /**
     * Supplier providing the data stream.
     * <p>
     * If the row count is known beforehand, use {@link SizedSupplier#of(java.util.function.Supplier, long)};
     * in this case, CacheManager skips the counting pass, reducing load time by ~50%.
     * If unknown, {@code SizedSupplier.unknown(...)} or a standard lambda can be passed.
     */
    private final SizedSupplier supplier;
    private final Function<CacheRow, String> keyExtractor;
    private final Function<CacheRow, byte[]> serializer;
    private final Function<byte[], V> deserializer;

    /**
     * The time to live duration for the entire cache version (when the supplier is called again to refresh).
     * If null or Duration.ZERO, no auto-refresh is triggered.
     */
    private final Duration ttl;

    // ── Shard / Record Sizing ──────────────────────────────────────────────
    @Builder.Default
    private final boolean dynamicSizing = false;

    @Builder.Default
    private final int maxKeyBytes = DEFAULT_MAX_KEY_BYTES;

    @Builder.Default
    private final int maxValueBytes = DEFAULT_MAX_VALUE_BYTES;

    // ── Caffeine In-Memory Layer ───────────────────────────────────────────

    /**
     * Maximum number of elements in the Caffeine cache layer.
     * If 0, the in-memory layer is disabled; every read goes directly to the mmap shards.
     */
    @Builder.Default
    private final int memoryCacheMaxSize = 0;

    /**
     * Caffeine write-after TTL: an element is removed after this duration
     * and reloaded from the shard on the next read.
     * If null, entries are evicted via LRU only when {@code memoryCacheMaxSize} is reached.
     */
    @Builder.Default
    private final Duration memoryCacheTtl = null;

    /**
     * Caffeine expire-after-access: an element is removed after this much idle time.
     * If null, it is inactive.
     * Can be combined with {@code memoryCacheTtl}.
     */
    @Builder.Default
    private final Duration memoryCacheIdleTtl = null;

    // ── Helpers ────────────────────────────────────────────────────────────
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
        return Long.BYTES + Short.BYTES + maxKeyBytes
                + Short.BYTES + maxValueBytes + Long.BYTES;
    }

    public int recordSize(int resolvedMaxKeyBytes, int resolvedMaxValueBytes) {
        return Long.BYTES + Short.BYTES + resolvedMaxKeyBytes
                + Short.BYTES + resolvedMaxValueBytes + Long.BYTES;
    }
}