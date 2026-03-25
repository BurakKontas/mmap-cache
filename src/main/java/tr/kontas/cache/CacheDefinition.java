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

    // ── Temel tanım ──────────────────────────────────────────────────────────
    private final String name;
    private final Supplier<Stream<CacheRow>> supplier;
    private final Function<CacheRow, String> keyExtractor;
    private final Function<CacheRow, byte[]> serializer;
    private final Function<byte[], V> deserializer;

    /**
     * Tüm cache versiyonunun yenileneceği süre (supplier yeniden çağrılır).
     * Null veya Duration.ZERO ise otomatik yenileme yapılmaz.
     */
    private final Duration ttl;

    // ── Shard / kayıt boyutu ─────────────────────────────────────────────────
    @Builder.Default
    private final boolean dynamicSizing = false;

    @Builder.Default
    private final int maxKeyBytes = DEFAULT_MAX_KEY_BYTES;

    @Builder.Default
    private final int maxValueBytes = DEFAULT_MAX_VALUE_BYTES;

    // ── Caffeine in-memory katmanı ───────────────────────────────────────────

    /**
     * Caffeine cache'inin maksimum eleman sayısı.
     * 0 verilirse in-memory katman devre dışı kalır; her okuma doğrudan
     * mmap shard'a gider.
     */
    @Builder.Default
    private final int memoryCacheMaxSize = 0;

    /**
     * Caffeine write-after TTL: bir eleman bu süre sonra cache'den düşer
     * ve bir sonraki okumada shard'dan yeniden yüklenir.
     * Null verilirse entry'ler yalnızca {@code memoryCacheMaxSize} dolunca
     * LRU politikasıyla çıkarılır; zamanlı çıkarma olmaz.
     */
    @Builder.Default
    private final Duration memoryCacheTtl = null;

    /**
     * Caffeine expire-after-access: son erişimden bu kadar süre sonra
     * entry cache'den düşer. Null verilirse aktif değildir.
     * {@code memoryCacheTtl} ile birlikte kullanılabilir.
     */
    @Builder.Default
    private final Duration memoryCacheIdleTtl = null;

    // ── Yardımcılar ──────────────────────────────────────────────────────────
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