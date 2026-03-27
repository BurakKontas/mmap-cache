package tr.kontas.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Immutable snapshot of one cache version.
 * <p>
 * Holds data shards, index shards, creation metadata, and the optional
 * in-memory L1 cache used during read operations.
 *
 * @param <V> deserialized value type
 */
@Slf4j
@Getter
public final class CacheVersion<V> {

    private final Path versionDir;
    private final CacheShard[] shards;
    private final Map<String, CacheLocation>[] indexShards;
    private final long createdAt;

    /**
     * Constructs a new cache version.
     *
     * @param versionDir  path to the version directory on disk
     * @param shards      array of data shards for this version
     * @param indexShards array of index shard maps (may be null or empty)
     * @param definition  the cache definition used to build this version
     */
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
     *
     * @param versionDir path to the version directory on disk
     * @param shards     array of data shards for this version
     * @param index      single index map kept for backwards compatibility (may be null)
     * @param definition the cache definition used to build this version
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

    /**
     * Caffeine in-memory layer.
     * Null if memoryCacheMaxSize == 0; reads go directly to the shard.
     */
    private final Cache<String, V> memoryCache;

    // ── Reader drain infrastructure ──────────────────────────────────────────
    //
    // FIX: replaced the busy-wait loop in CacheManager.cleanupOldVersion
    // (Thread.sleep(50) polling hasActiveReaders()) with a monitor-based
    // wait/notify mechanism.  When the last reader releases, drainMonitor is
    // notified, allowing the cleanup thread to wake immediately instead of
    // burning CPU in a polling loop.
    //
    // Protocol:
    //   1. acquireReader()  — called inside the StampedLock read section in
    //      CacheManager.getFromSlot before any data access.
    //   2. releaseReader()  — called in the finally block of getFromSlot.
    //   3. awaitDrained()   — called by cleanupOldVersion AFTER the version
    //      has been swapped out; waits until readerCount == 0.
    //   4. close()          — called after awaitDrained() returns.
    //
    private final AtomicInteger readerCount = new AtomicInteger();
    private final Object drainMonitor = new Object();

    /**
     * @deprecated Use {@link #getIndexShards()} for sharded access.
     *
     * @return first index shard for backward compatibility, or null when unavailable
     */
    @Deprecated
    public Map<String, CacheLocation> getIndex() {
        return indexShards != null && indexShards.length > 0 ? indexShards[0] : null;
    }

    /**
     * Returns the index shard array backing this cache version.
     *
     * @return index shard maps (may be null or empty)
     */
    public Map<String, CacheLocation>[] getIndexShards() {
        return indexShards;
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

    /**
     * Reads a value from the in-memory L1 cache if present.
     *
     * @param key lookup key
     * @return cached value or null when not present or memory cache is disabled
     */
    public V getFromMemory(String key) {
        if (memoryCache == null) return null;
        return memoryCache.getIfPresent(key);
    }

    /**
     * Puts a value into the in-memory L1 cache when enabled.
     *
     * @param key   lookup key
     * @param value value to cache
     */
    public void putToMemory(String key, V value) {
        if (memoryCache == null) return;
        memoryCache.put(key, value);
    }

    // ── Reader counter ───────────────────────────────────────────────────────

    /**
     * Increment the active reader count for this version. Call when starting a read.
     * Must be called while holding the {@code CacheSlot} read lock so that the
     * increment is atomic with respect to a concurrent version swap.
     */
    public void acquireReader() {
        readerCount.incrementAndGet();
    }

    /**
     * Decrement the active reader count for this version. Call when finishing a read.
     * When the count reaches zero the drain monitor is notified so that any thread
     * blocked in {@link #awaitDrained()} can wake up immediately.
     */
    public void releaseReader() {
        if (readerCount.decrementAndGet() == 0) {
            synchronized (drainMonitor) {
                drainMonitor.notifyAll();
            }
        }
    }

    /**
     * Returns whether this version currently has active readers.
     *
     * @return true when there are active readers
     */
    public boolean hasActiveReaders() {
        return readerCount.get() > 0;
    }

    /**
     * Blocks the calling thread until all active readers have released this version.
     *
     * <p>This replaces the previous busy-wait loop
     * ({@code while (hasActiveReaders()) Thread.sleep(50)}) in
     * {@code CacheManager.cleanupOldVersion}.  The cleanup thread now sleeps
     * in the OS wait queue and is woken by {@link #releaseReader()} as soon as
     * the last reader finishes — typically sub-millisecond latency vs. an
     * average 25 ms delay with the old approach.
     *
     * <p>A 100 ms timeout is used as a safety net in case a notification is
     * missed due to a spurious wake-up; the loop re-evaluates the condition
     * after each wake-up.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public void awaitDrained() throws InterruptedException {
        if (!hasActiveReaders()) return;
        synchronized (drainMonitor) {
            while (readerCount.get() > 0) {
                drainMonitor.wait(100);
            }
        }
    }

    // ── Shutdown ─────────────────────────────────────────────────────────────

    /**
     * Closes the shards and the Chronicle Map index.
     * Caffeine cache is cleaned by the GC; explicit invalidation is sufficient.
     *
     * <p>Callers must ensure {@link #awaitDrained()} has returned before calling
     * {@code close()} to avoid closing resources that are still in use.
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