package tr.kontas.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Getter
public final class CacheVersion<V> {

    private final Path versionDir;
    private final CacheShard[] shards;
    private final Map<String, CacheLocation> index;
    private final long createdAt;

    /**
     * Caffeine in-memory katmanı.
     * memoryCacheMaxSize == 0 ise null'dır; okuma doğrudan shard'a gider.
     */
    private final Cache<String, V> memoryCache;

    private final AtomicInteger readerCount = new AtomicInteger();

    public CacheVersion(
            Path versionDir,
            CacheShard[] shards,
            Map<String, CacheLocation> index,
            CacheDefinition<V> definition
    ) {
        this.versionDir = versionDir;
        this.shards = shards;
        this.index = index;
        this.createdAt = System.currentTimeMillis();
        this.memoryCache = buildCache(definition);
    }

    // ── Caffeine cache builder ───────────────────────────────────────────────

    private static <V> Cache<String, V> buildCache(CacheDefinition<V> def) {
        int maxSize = def.getMemoryCacheMaxSize();
        if (maxSize <= 0) return null;

        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(maxSize);

        if (def.getMemoryCacheTtl() != null && !def.getMemoryCacheTtl().isZero()) {
            builder.expireAfterWrite(def.getMemoryCacheTtl());
        }

        if (def.getMemoryCacheIdleTtl() != null && !def.getMemoryCacheIdleTtl().isZero()) {
            builder.expireAfterAccess(def.getMemoryCacheIdleTtl());
        }

        log.debug("Caffeine cache built: maxSize={}, ttlAfterWrite={}, ttlAfterAccess={}",
                maxSize, def.getMemoryCacheTtl(), def.getMemoryCacheIdleTtl());

        return builder.build();
    }

    // ── Cache erişim yardımcıları ────────────────────────────────────────────

    public V getFromMemory(String key) {
        if (memoryCache == null) return null;
        return memoryCache.getIfPresent(key);
    }

    public void putToMemory(String key, V value) {
        if (memoryCache == null) return;
        memoryCache.put(key, value);
    }

    // ── Reader sayacı ────────────────────────────────────────────────────────

    public void acquireReader() {
        readerCount.incrementAndGet();
    }

    public void releaseReader() {
        readerCount.decrementAndGet();
    }

    public boolean hasActiveReaders() {
        return readerCount.get() > 0;
    }

    // ── Kapatma ─────────────────────────────────────────────────────────────

    /**
     * Shardları ve Chronicle Map index'ini kapatır.
     * Caffeine cache GC tarafından temizlenir; explicit invalidation yeterlidir.
     */
    public void close() {
        // 1. Caffeine'i geçersiz kıl
        if (memoryCache != null) {
            memoryCache.invalidateAll();
        }

        // 2. Shard dosyalarını kapat
        for (CacheShard shard : shards) {
            try {
                shard.close();
            } catch (Exception e) {
                log.warn("Failed to close shard", e);
            }
        }

        // 3. Off-heap Chronicle Map'i kapat (varsa)
        if (index instanceof Closeable) {
            try {
                ((Closeable) index).close();
                log.debug("Chronicle Map index closed for version '{}'", versionDir.getFileName());
            } catch (Exception e) {
                log.warn("Failed to close Chronicle Map index for version '{}'",
                        versionDir.getFileName(), e);
            }
        }
    }
}