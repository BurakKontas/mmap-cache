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
    private final Map<String, CacheLocation>[] indexShards;
    private final long createdAt;

    /**
     * @deprecated Use {@link #getIndexShards()} for sharded access.
     */
    @Deprecated
    public Map<String, CacheLocation> getIndex() {
        return indexShards != null && indexShards.length > 0 ? indexShards[0] : null;
    }

    /**
     * Caffeine in-memory layer.
     * Null if memoryCacheMaxSize == 0; reads go directly to the shard.
     */
    private final Cache<String, V> memoryCache;

    private final AtomicInteger readerCount = new AtomicInteger();

    public CacheVersion(
            Path versionDir,
            CacheShard[] shards,
            Map<String, CacheLocation>[] indexShards,
            CacheDefinition<V> definition
    ) {
        this.versionDir = versionDir;
        this.shards = shards;
        this.indexShards = indexShards;
        this.createdAt = System.currentTimeMillis();
        this.memoryCache = buildCache(definition);
    }

    /**
     * Backward-compatibility/Test constructor.
     */
    @SuppressWarnings("unchecked")
    public CacheVersion(
            Path versionDir,
            CacheShard[] shards,
            Map<String, CacheLocation> index,
            CacheDefinition<V> definition
    ) {
        this(versionDir, shards, index == null ? new Map[0] : new Map[]{index}, definition);
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

    // ── Cache access helpers  ────────────────────────────────────────────────

    public V getFromMemory(String key) {
        if (memoryCache == null) return null;
        return memoryCache.getIfPresent(key);
    }

    public void putToMemory(String key, V value) {
        if (memoryCache == null) return;
        memoryCache.put(key, value);
    }

    // ── Reader counter ───────────────────────────────────────────────────────

    public void acquireReader() {
        readerCount.incrementAndGet();
    }

    public void releaseReader() {
        readerCount.decrementAndGet();
    }

    public boolean hasActiveReaders() {
        return readerCount.get() > 0;
    }

    // ── Shutdown ─────────────────────────────────────────────────────────────

    /**
     * Closes the shards and the Chronicle Map index.
     * Caffeine cache is cleaned by the GC; explicit invalidation is sufficient.
     */
    public void close() {
        // 1. Invalidate Caffeine cache
        if (memoryCache != null) {
            memoryCache.invalidateAll();
        }

        // 2. Close shard files
        for (CacheShard shard : shards) {
            try {
                shard.close();
            } catch (Exception e) {
                log.warn("Failed to close shard", e);
            }
        }

        // 3. Close off-heap Chronicle Map index (if any)
        if (indexShards != null) {
            for (int i = 0; i < indexShards.length; i++) {
                Map<String, CacheLocation> index = indexShards[i];
                if (index instanceof Closeable) {
                    try {
                        ((Closeable) index).close();
                        log.debug("Chronicle Map index {} closed for version '{}'", i, versionDir.getFileName());
                    } catch (Exception e) {
                        log.warn("Failed to close Chronicle Map index {} for version '{}'",
                                i, versionDir.getFileName(), e);
                    }
                }
            }
        }
    }
}