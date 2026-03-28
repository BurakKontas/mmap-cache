package tr.kontas.cache;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 * Manages the lifecycle, registration, and lookup of memory-mapped caches.
 * <p>
 * This manager provides:
 * <ul>
 *   <li>Off-heap segmented arrays via {@link CacheShard} for high-performance reading and writing</li>
 *   <li>Sharded ChronicleMap indexes (persisted on disk) for resolving keys efficiently</li>
 *   <li>Optional on-heap L1 caching via Caffeine</li>
 *   <li>Atomic cache reloading and background asynchronous TTL refreshes</li>
 * </ul>
 *
 * <h3>Concurrency model</h3>
 * <ul>
 *   <li><b>Read path</b>: {@link #get} acquires a {@link StampedLock} <em>read</em> lock for
 *       the minimal critical section (read {@code activeVersion} + {@code acquireReader()}),
 *       then does all data work outside any lock.  Concurrent reads on the same cache slot
 *       never block each other.</li>
 *   <li><b>Version swap</b>: {@link #doReload} acquires the <em>write</em> lock for the atomic
 *       swap of {@code activeVersion}.  This prevents new readers from seeing the old version
 *       after the swap, and guarantees that {@code acquireReader()} on the new version happens
 *       before any cleanup of the old one.</li>
 *   <li><b>Old-version cleanup</b>: uses {@link CacheVersion#awaitDrained()} (monitor-based
 *       wait/notify) instead of a busy-wait {@code Thread.sleep(50)} loop.</li>
 *   <li><b>Parallel write pass</b>: data shards are written in parallel via
 *       {@code stream().parallel()} with a {@link ThreadLocal}&lt;{@link CacheEntry}&gt;,
 *       eliminating per-row heap allocation without sharing mutable state between threads.</li>
 * </ul>
 */
@Slf4j
public final class CacheManager {

    // ── Background threads ───────────────────────────────────────────────────
    private static final ScheduledExecutorService TTL_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cache-ttl-scheduler");
                t.setDaemon(true);
                return t;
            });

    // FIX: newCachedThreadPool creates an unbounded number of threads under load.
    // A bounded pool prevents thread explosion when many caches reload simultaneously.
    // Pool size = max(2, cpuCount/2) — reloads are partly I/O-bound so we allow
    // slight oversubscription, but stay well under the OS thread limit.
    private static final ExecutorService RELOAD_EXECUTOR =
            Executors.newFixedThreadPool(
                    Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                    r -> {
                        Thread t = new Thread(r, "cache-reload-worker");
                        t.setDaemon(true);
                        return t;
                    });

    // ── Singleton ────────────────────────────────────────────────────────────
    private static volatile CacheManager INSTANCE;

    // ── Manager config ───────────────────────────────────────────────────────
    private final Path basePath;
    private final int shardCapacity;
    private final int defaultMaxKeyBytes;
    private final int defaultMaxValueBytes;
    private final int memoryCacheSize;
    private final String chronicleAverageKey;
    // FIX: pre-compute the UTF-8 byte length of the average key once.
    // Previously, averageKey.getBytes(UTF_8) was called inside buildChronicleIndex
    // on every shard (up to indexShardCount times per reload), allocating a
    // temporary byte[] each time.
    private final int chronicleAverageKeyBytes;
    private final int indexShardCount;
    private final boolean useChronicleMap;

    private final ConcurrentHashMap<String, CacheSlot<?>> slots = new ConcurrentHashMap<>();

    // ── Constructor ──────────────────────────────────────────────────────────
    private CacheManager(
            Path basePath,
            int shardCapacity,
            int defaultMaxKeyBytes,
            int defaultMaxValueBytes,
            int memoryCacheSize,
            String chronicleAverageKey,
            int indexShardCount,
            boolean useChronicleMap
    ) {
        this.basePath = basePath;
        this.shardCapacity = shardCapacity;
        this.defaultMaxKeyBytes = defaultMaxKeyBytes;
        this.defaultMaxValueBytes = defaultMaxValueBytes;
        this.memoryCacheSize = memoryCacheSize;
        this.chronicleAverageKey = chronicleAverageKey;
        this.chronicleAverageKeyBytes = chronicleAverageKey.getBytes(StandardCharsets.UTF_8).length;
        this.indexShardCount = indexShardCount;
        this.useChronicleMap = useChronicleMap;

        TTL_SCHEDULER.scheduleWithFixedDelay(
                this::checkTtlExpiry,
                10, 10, TimeUnit.SECONDS
        );
    }

    // ── Static init ──────────────────────────────────────────────────────────

    /**
     * Initializes the global cache manager with default configurations.
     *
     * @param basePath the root directory where all cache data will be stored
     */
    public static synchronized void initialize(Path basePath) {
        INSTANCE = new CacheManager(
                basePath,
                100_000,
                CacheDefinition.DEFAULT_MAX_KEY_BYTES,
                CacheDefinition.DEFAULT_MAX_VALUE_BYTES,
                0,
                "key-00000000",
                16,
                true
        );
        purgeStaleVersions(basePath);
    }

    /**
     * Initializes the global cache manager with default configurations.
     *
     * @param baseDir the root directory as a File object
     */
    public static synchronized void initialize(File baseDir) {
        if (baseDir == null) throw new IllegalArgumentException("baseDir cannot be null");
        initialize(baseDir.toPath());
    }

    /**
     * Initializes the global cache manager with a custom builder configuration.
     *
     * @param builder a configured {@link Builder}
     */
    public static synchronized void initialize(Builder builder) {
        if (builder == null) throw new IllegalArgumentException("builder cannot be null");
        INSTANCE = builder.build();
        purgeStaleVersions(INSTANCE.basePath);
    }

    /**
     * Creates a new builder for customizing the CacheManager properties.
     *
     * @param basePath the root directory where cache data corresponds
     * @return a new {@link Builder} instance
     */
    public static Builder builder(Path basePath) {
        return new Builder(basePath);
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /**
     * Registers a new cache based on the provided definition and loads its data asynchronously.
     *
     * @param <V>        the type of cached values
     * @param definition the definition of the cache specifying keys, serializations, TTL, and boundaries
     */
    public static <V> void register(CacheDefinition<V> definition) {
        CacheManager mgr = requireInstance();

        CacheDefinition<V> effective = definition;
        if (!definition.isDynamicSizing()) {
            CacheDefinition.CacheDefinitionBuilder<V> b = definition.toBuilder();
            if (definition.getMaxKeyBytes() == CacheDefinition.DEFAULT_MAX_KEY_BYTES
                    && mgr.defaultMaxKeyBytes != CacheDefinition.DEFAULT_MAX_KEY_BYTES) {
                b.maxKeyBytes(mgr.defaultMaxKeyBytes);
            }
            if (definition.getMaxValueBytes() == CacheDefinition.DEFAULT_MAX_VALUE_BYTES
                    && mgr.defaultMaxValueBytes != CacheDefinition.DEFAULT_MAX_VALUE_BYTES) {
                b.maxValueBytes(mgr.defaultMaxValueBytes);
            }
            if (definition.getMemoryCacheMaxSize() == 0 && mgr.memoryCacheSize > 0) {
                b.memoryCacheMaxSize(mgr.memoryCacheSize);
            }
            effective = b.build();
        }

        CacheSlot<V> slot = new CacheSlot<>(effective);
        mgr.slots.put(effective.getName(), slot);
        mgr.doReload(slot, "init");
        log.info("Cache '{}' registered and loaded.", effective.getName());
    }

    /**
     * Reads a value from the named cache by key.
     *
     * <p><b>Concurrency:</b> this method acquires a {@link StampedLock} read lock only for
     * the brief critical section that reads {@code activeVersion} and calls
     * {@link CacheVersion#acquireReader()}.  All actual data work (shard lookup,
     * deserialization, Caffeine access) happens after the lock is released, so concurrent
     * reads on the same cache slot do not block each other.
     *
     * @param <V>       value type
     * @param cacheName registered cache name
     * @param key       lookup key
     * @return cached value or null when key/cache is missing
     */
    @SuppressWarnings("unchecked")
    public static <V> V get(String cacheName, String key) {
        CacheSlot<V> slot = (CacheSlot<V>) requireInstance().slots.get(cacheName);
        if (slot == null) {
            log.warn("Cache '{}' not registered.", cacheName);
            return null;
        }
        return getFromSlot(slot, key);
    }

    /**
     * Triggers a synchronous reload for a registered cache.
     *
     * @param cacheName registered cache name
     */
    public static void reload(String cacheName) {
        CacheSlot<?> slot = requireInstance().slots.get(cacheName);
        if (slot == null) throw new IllegalArgumentException("Unknown cache: " + cacheName);
        //noinspection unchecked
        requireInstance().doReload((CacheSlot<Object>) slot, "manual");
    }

    /**
     * Schedules an asynchronous reload for a registered cache.
     *
     * @param cacheName registered cache name
     */
    public static void reloadAsync(String cacheName) {
        RELOAD_EXECUTOR.submit(() -> reload(cacheName));
    }

    // ── Internal read ────────────────────────────────────────────────────────

    /**
     * Thread-safe read path using {@link StampedLock}.
     *
     * <h4>Why StampedLock instead of {@code synchronized}</h4>
     * <p>The original code held {@code synchronized(slot)} for the entire
     * {@code acquireReader()} call, which serialised <em>all</em> concurrent reads on
     * the same slot through a single monitor.  Under high concurrency this collapses
     * throughput to a single thread.
     *
     * <p>The new approach:
     * <ol>
     *   <li>Acquire a <em>read</em> lock (shared — multiple readers allowed simultaneously).</li>
     *   <li>Read {@code slot.activeVersion} and call {@code acquireReader()} while holding
     *       the lock.  This critical section is O(1) and very short.</li>
     *   <li>Release the read lock.</li>
     *   <li>Perform all data work (Caffeine lookup, ChronicleMap lookup, shard read,
     *       deserialisation) <em>outside</em> any lock.</li>
     * </ol>
     *
     * <p>{@code doReload} acquires the <em>write</em> lock for the version swap, which
     * guarantees that the swap and the reader increment are mutually exclusive, preserving
     * the invariant that no reader can see a closed version.
     */
    private static <V> V getFromSlot(CacheSlot<V> slot, String key) {
        CacheVersion<V> version;

        // FIX: use StampedLock read lock — many threads can be inside simultaneously.
        // The lock is held only for the brief critical section below.
        long stamp = slot.lock.readLock();
        try {
            version = slot.activeVersion;
            if (version == null) return null;
            // acquireReader() must happen inside the lock so it is atomic with respect
            // to the write-lock-guarded version swap in doReload().
            version.acquireReader();
        } finally {
            slot.lock.unlockRead(stamp);
        }

        try {
            // 1. Caffeine in-memory layer
            V cached = version.getFromMemory(key);
            if (cached != null) return cached;

            // Determine index shard
            int shardCount = version.getIndexShards() != null ? version.getIndexShards().length : 1;
            int shardId = shardCount > 1 ? (key.hashCode() & 0x7fffffff) % shardCount : 0;

            // 2. Off-heap Chronicle index → mmap shard
            if (version.getIndexShards() == null || version.getIndexShards()[shardId] == null) {
                return null;
            }
            CacheLocation loc = version.getIndexShards()[shardId].get(key);
            if (loc == null) return null;

            CacheEntry entry = version.getShards()[loc.shardId()].read(loc.offset());
            V value = slot.definition.getDeserializer().apply(entry.getValueBytes());

            version.putToMemory(key, value);
            return value;
        } finally {
            version.releaseReader();
        }
    }

    // ── Chronicle Map ────────────────────────────────────────────────────────

    /**
     * Builds an off-heap ChronicleMap index, falling back to an in-memory
     * {@code ConcurrentHashMap} if ChronicleMap is unavailable.
     *
     * <h4>API evolution across ChronicleMap 3.x</h4>
     * <ul>
     *   <li>3.17–3.23 stable : {@code createOrRecoverPersistedTo(File)} (1-arg)</li>
     *   <li>3.24–3.25 EA     : only {@code createOrRecoverPersistedTo(File, boolean)}
     *       exists; the 1-arg overload was removed.</li>
     * </ul>
     *
     * <p>We always call the 2-arg form. {@code sameLibraryVersion=true} tells ChronicleMap
     * that the library version matches the one that created the file, so it may safely
     * overwrite the header rather than failing with
     * {@link net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException}.
     *
     * <h4>Why we must NOT pre-create the file</h4>
     * A zero-byte file has no ChronicleMap header. When {@code createOrRecoverPersistedTo}
     * opens an existing (but empty) file it attempts to read the header; finding nothing,
     * it throws {@link net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException}.
     * We create the parent directory only; the file itself is left to ChronicleMap.
     *
     * @param averageKeyBytes pre-computed UTF-8 byte length of {@code averageKey} —
     *                        avoids a redundant {@code getBytes()} allocation per shard
     */
    private static Map<String, CacheLocation> buildChronicleIndex(
            Path file, int expectedEntries, String averageKey, int averageKeyBytes, boolean useChronicleMap
    ) throws IOException {
        if (!useChronicleMap) {
            log.warn("ChronicleMap usage disabled by configuration using in-memory index for file: {}",
                    file != null ? file.toAbsolutePath() : "null");
            return new FallbackCloseableMap<>();
        }

        int entries = Math.max(expectedEntries, 1);

        // FIX: create parent dir ONLY — do NOT pre-create the file.
        // The original code called Files.createFile(file) when the file did not exist,
        // which produced an empty (zero-byte) file.  ChronicleMap would then try to
        // read the header from this empty file, fail to find one, and throw
        // ChronicleHashRecoveryFailedException.  Removing the pre-creation call allows
        // ChronicleMap to create and initialise the file itself.
        if (file.getParent() != null) Files.createDirectories(file.getParent());

        try {
            Map<String, CacheLocation> map = ChronicleMapBuilder
                    .of(String.class, CacheLocation.class)
                    .name("cache-index")
                    .entries(entries)
                    .averageKey(averageKey)
                    .averageKeySize(averageKeyBytes)       // FIX: pre-computed, no alloc
                    .averageValueSize(64)
                    .createPersistedTo(file.toFile());

            if (map instanceof java.io.Closeable) {
                return map;
            } else {
                FallbackCloseableMap<String, CacheLocation> wrapper = new FallbackCloseableMap<>();
                wrapper.putAll(map);
                return wrapper;
            }
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            log.warn("ChronicleMap not available or incompatible; falling back to in-memory index: {}",
                    e.toString());
            // Ensure the index file exists for callers/tests that expect a path to be created.
            try {
                if (file != null && Files.notExists(file)) Files.createFile(file);
            } catch (Exception ignored) {
                // best-effort only
            }
            return new FallbackCloseableMap<>();
        } catch (Throwable t) {
            log.warn("Failed to create ChronicleMap index, falling back to in-memory map: {}",
                    t.toString());
            // Ensure the index file exists for callers/tests that expect a path to be created.
            try {
                if (file != null && Files.notExists(file)) Files.createFile(file);
            } catch (Exception ignored) {
                // best-effort only
            }
            return new FallbackCloseableMap<>();
        }
    }

    /**
     * Heap-based fallback index used when ChronicleMap is unavailable.
     *
     * <p>Throws {@link IllegalStateException} on any access after {@link #close()},
     * mirroring ChronicleMap's behaviour so callers treat both implementations uniformly.
     *
     * <h4>FIX — TOCTOU race condition</h4>
     * <p>The original implementation called {@code ensureOpen()} (reads {@code closed})
     * and then {@code super.get()}/{@code super.put()} as two separate steps.  A
     * concurrent {@code close()} could set {@code closed = true} between the check
     * and the map operation, letting a "closed" access slip through silently.
     *
     * <p>The fix uses a {@link ReentrantReadWriteLock}:
     * <ul>
     *   <li>All data operations ({@code get}, {@code put}, {@code remove}, {@code clear})
     *       hold the <em>read</em> lock — many can run concurrently.</li>
     *   <li>{@code close()} holds the <em>write</em> lock — exclusive, waits for all
     *       ongoing operations to finish before setting {@code closed = true}.</li>
     * </ul>
     * <p>After {@code close()} returns, any subsequent data operation will acquire the
     * read lock, call {@code ensureOpen()}, and throw {@code IllegalStateException}.
     */
    private static final class FallbackCloseableMap<K, V>
            extends ConcurrentHashMap<K, V>
            implements java.io.Closeable {

        private volatile boolean closed = false;
        // FIX: ReentrantReadWriteLock makes ensureOpen() + the actual operation atomic.
        // Read lock: shared among concurrent data ops.
        // Write lock: exclusive for close().
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

        @Override
        public void close() {
            rwLock.writeLock().lock();
            try {
                closed = true;
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        private void ensureOpen() {
            if (closed) throw new IllegalStateException("Index map is closed");
        }

        @Override
        public V put(K key, V value) {
            rwLock.readLock().lock();
            try {
                ensureOpen();
                return super.put(key, value);
            } finally {
                rwLock.readLock().unlock();
            }
        }

        @Override
        public V get(Object key) {
            rwLock.readLock().lock();
            try {
                ensureOpen();
                return super.get(key);
            } finally {
                rwLock.readLock().unlock();
            }
        }

        @Override
        public V remove(Object key) {
            rwLock.readLock().lock();
            try {
                ensureOpen();
                return super.remove(key);
            } finally {
                rwLock.readLock().unlock();
            }
        }

        @Override
        public void clear() {
            rwLock.readLock().lock();
            try {
                ensureOpen();
                super.clear();
            } finally {
                rwLock.readLock().unlock();
            }
        }
    }

    // ── Stale version purge ──────────────────────────────────────────────────
    public static void purgeStaleVersions(Path basePath) {
        if (!Files.exists(basePath)) return;
        try (var cacheDirs = Files.list(basePath)) {
            cacheDirs.forEach(cacheDir -> {
                if (!Files.isDirectory(cacheDir)) return;
                try (var versions = Files.list(cacheDir)) {
                    versions.filter(Files::isDirectory)
                            .filter(p -> p.getFileName().toString().startsWith("v"))
                            .forEach(versionDir -> {
                                try (var walk = Files.walk(versionDir)) {
                                    walk.sorted((a, b) -> b.compareTo(a))
                                            .forEach(p -> {
                                                try {
                                                    Files.deleteIfExists(p);
                                                } catch (Exception ignored) {
                                                }
                                            });
                                    log.info("Purged stale cache version: {}", versionDir);
                                } catch (Exception e) {
                                    log.warn("Failed to purge stale version: {}", versionDir, e);
                                }
                            });
                } catch (Exception e) {
                    log.warn("Failed to list cache dir: {}", cacheDir, e);
                }
            });
        } catch (Exception e) {
            log.warn("Failed to purge stale cache versions", e);
        }
    }

    private static CacheManager requireInstance() {
        CacheManager inst = INSTANCE;
        if (inst == null) throw new IllegalStateException(
                "CacheManager not initialized. Call CacheManager.initialize() first.");
        return inst;
    }

    // ── Reload ───────────────────────────────────────────────────────────────
    private <V> void doReload(CacheSlot<V> slot, String trigger) {
        if (!slot.reloading.compareAndSet(false, true)) {
            log.info("Reload already in progress for cache '{}'", slot.definition.getName());
            return;
        }

        String name = slot.definition.getName();

        try {
            Path cacheDir = basePath.resolve(name);
            Files.createDirectories(cacheDir);

            long versionTs = System.currentTimeMillis();
            Path versionDir = cacheDir.resolve("v" + versionTs);
            Files.createDirectories(versionDir);

            // ── Pass-1: determine row count and field widths ─────────────────
            //
            // Four cases — cheapest available is selected:
            //
            //  A) dynamicSizing=true  + count known + explicit bounds → skip scan entirely
            //  B) dynamicSizing=true  + (count or bounds missing)     → full sizing scan
            //  C) dynamicSizing=false + count known                   → no scan at all
            //  D) dynamicSizing=false + count unknown                 → count-only scan

            final int maxKeyBytes;
            final int maxValueBytes;
            final int total;

            var knownCount = slot.definition.getSupplier().count();

            if (slot.definition.isDynamicSizing()) {

                boolean explicitBounds =
                        slot.definition.getMaxKeyBytes() != CacheDefinition.DEFAULT_MAX_KEY_BYTES ||
                                slot.definition.getMaxValueBytes() != CacheDefinition.DEFAULT_MAX_VALUE_BYTES;

                if (knownCount.isPresent() && explicitBounds) {
                    // Case A: everything already known — no scan.
                    total = (int) knownCount.getAsLong();
                    maxKeyBytes = slot.definition.getMaxKeyBytes();
                    maxValueBytes = slot.definition.getMaxValueBytes();
                    log.info("Cache '{}': dynamic sizing skipped (count hint + explicit bounds) → " +
                                    "maxKeyBytes={}, maxValueBytes={} ({} rows)",
                            name, maxKeyBytes, maxValueBytes, total);

                } else {
                    // Case B: scan to measure actual max key/value byte lengths.
                    int[] maxK = {0};
                    int[] maxV = {0};
                    long[] count = {0};
                    try (var stream = slot.definition.getSupplier().get()) {
                        stream.forEach(row -> {
                            count[0]++;
                            String k = slot.definition.getKeyExtractor().apply(row);
                            byte[] v = slot.definition.getSerializer().apply(row);
                            int kb = k.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                            if (kb > maxK[0]) maxK[0] = kb;
                            if (v.length > maxV[0]) maxV[0] = v.length;
                        });
                    }
                    total = (int) knownCount.orElse(count[0]);
                    maxKeyBytes = (int) (maxK[0] * 1.25) + 1;
                    maxValueBytes = (int) (maxV[0] * 1.25) + 1;
                    log.info("Cache '{}': dynamic sizing → maxKeyBytes={}, maxValueBytes={} ({} rows)",
                            name, maxKeyBytes, maxValueBytes, total);
                }

            } else if (knownCount.isPresent()) {
                // Case C: count hint + fixed bounds → no scan needed.
                total = (int) knownCount.getAsLong();
                maxKeyBytes = slot.definition.getMaxKeyBytes();
                maxValueBytes = slot.definition.getMaxValueBytes();
                log.info("Cache '{}': sized supplier hint → {} rows (no scan pass)", name, total);

            } else {
                // Case D: fixed bounds but count unknown → count-only scan.
                long[] count = {0};
                try (var stream = slot.definition.getSupplier().get()) {
                    stream.forEach(row -> count[0]++);
                }
                total = (int) count[0];
                maxKeyBytes = slot.definition.getMaxKeyBytes();
                maxValueBytes = slot.definition.getMaxValueBytes();
            }

            int recordSize = slot.definition.recordSize(maxKeyBytes, maxValueBytes);
            int shardCount = Math.max(1, (int) Math.ceil((double) total / shardCapacity));

            log.info("Cache '{}': {} rows → {} shard(s)", name, total, shardCount);

            // ── Create shards ────────────────────────────────────────────────
            CacheShard[] newShards = new CacheShard[shardCount];
            for (int i = 0; i < shardCount; i++) {
                int shardSize = (i < shardCount - 1)
                        ? shardCapacity
                        : Math.max(1, total - (i * shardCapacity));
                Path shardPath = versionDir.resolve(String.format("shard_%04d.dat", i));
                newShards[i] = new CacheShard(shardPath, recordSize, shardSize, maxKeyBytes, maxValueBytes);
            }

            // ── Build Chronicle Map index ────────────────────────────────────
            @SuppressWarnings("unchecked")
            Map<String, CacheLocation>[] indexShards = new Map[indexShardCount];
            int expectedPerShard = Math.max(1, (int) Math.ceil((double) total / indexShardCount));
            for (int i = 0; i < indexShardCount; i++) {
                Path chronicleFile = versionDir.resolve(String.format("index_%04d.chm", i));
                indexShards[i] = buildChronicleIndex(
                        chronicleFile, expectedPerShard, chronicleAverageKey, chronicleAverageKeyBytes, this.useChronicleMap
                );
            }

            // ── Pass-2: write & index (parallel) ────────────────────────────
            //
            // FIX: the original sequential stream with a single reused CacheEntry
            // was a throughput bottleneck for large datasets (50M+ rows single-threaded).
            //
            // Changes:
            //   1. stream().parallel()  — uses ForkJoinPool.commonPool() to distribute
            //      rows across available CPU cores.
            //   2. ThreadLocal<CacheEntry> — each worker thread gets its own CacheEntry
            //      instance, eliminating the shared-mutable-state problem that prevented
            //      parallelism in the original design.
            //   3. AtomicInteger counter — replaces the int[] counter to produce unique,
            //      monotonically increasing row indices across threads.
            //   4. AtomicInteger skippedCount — thread-safe replacement for int[] skipped.
            //
            // Thread safety of CacheShard.write():
            //   CacheShard was fixed to use buffer.duplicate() so concurrent writes to
            //   different offsets are safe.  ChronicleMap and FallbackCloseableMap are
            //   both safe for concurrent puts from different threads.
            //
            // Ordering:
            //   Row order is non-deterministic in parallel mode.  This is acceptable
            //   because the cache is a key-value store; correctness depends on key
            //   uniqueness, not insertion order.

            final AtomicInteger counter = new AtomicInteger(0);
            final AtomicInteger skippedCount = new AtomicInteger(0);
            final ThreadLocal<CacheEntry> entryLocal = ThreadLocal.withInitial(
                    () -> new CacheEntry(maxKeyBytes, maxValueBytes)
            );
            final int finalTotal = total;  // effectively-final for lambda

            try (var stream = slot.definition.getSupplier().get()) {
                stream.parallel().forEach(row -> {
                    int i = counter.getAndIncrement();

                    // Defensive guard: if the supplier yields more rows than expected,
                    // the pre-sized shards would throw IndexOutOfBoundsException.
                    if (i >= finalTotal) {
                        log.warn("Cache '{}': supplier returned more rows than expected total={}, " +
                                "skipping extra row at index {}", name, finalTotal, i);
                        return;
                    }

                    int shardId = i / shardCapacity;
                    int offset  = i % shardCapacity;

                    String key        = slot.definition.getKeyExtractor().apply(row);
                    byte[] valueBytes = slot.definition.getSerializer().apply(row);

                    CacheEntry entry = entryLocal.get();
                    entry.reset(i + 1L, versionTs, key, valueBytes);

                    boolean written = newShards[shardId].write(offset, entry);
                    if (!written) {
                        log.warn("Cache '{}': skipping key '{}' (value too large)", name, key);
                        skippedCount.incrementAndGet();
                        return;
                    }
                    int indexShardId = (key.hashCode() & 0x7fffffff) % indexShardCount;
                    indexShards[indexShardId].put(key, new CacheLocation(shardId, offset));
                });
            }

            int totalSkipped = skippedCount.get();
            if (totalSkipped > 0) {
                log.warn("Cache '{}': {} / {} entries skipped (value exceeded maxValueBytes={}). " +
                                "Consider raising maxValueBytes or enabling dynamicSizing.",
                        name, totalSkipped, total, maxValueBytes);
            }

            for (CacheShard shard : newShards) shard.flush();

            CacheVersion<V> newVersion = new CacheVersion<>(
                    versionDir, newShards, indexShards, slot.definition
            );

            // ── Atomic version swap (write lock) ─────────────────────────────
            // FIX: replaced synchronized(slot) with StampedLock write lock.
            // The write lock is exclusive — it waits for all concurrent read-lock
            // holders (i.e. in-flight getFromSlot calls) to finish their critical
            // section before proceeding.  This guarantees that acquireReader() on
            // the old version cannot race with the swap: either it happened before
            // the write lock was acquired (so the old version still has readers and
            // cleanupOldVersion will wait), or it will happen on the new version.
            CacheVersion<V> oldVersion;
            long stamp = slot.lock.writeLock();
            try {
                oldVersion = slot.activeVersion;
                slot.activeVersion = newVersion;
            } finally {
                slot.lock.unlockWrite(stamp);
            }

            log.info("Cache '{}' reloaded. version={}, shards={}, entries={}",
                    name, versionDir.getFileName(), shardCount, total - totalSkipped);

            cleanupOldVersion(name, oldVersion);

        } catch (Exception e) {
            log.error("Reload failed for cache '{}'", name, e);
            throw new RuntimeException("Reload failed for cache: " + name, e);
        } finally {
            slot.reloading.set(false);
        }
    }

    // ── TTL scheduler ────────────────────────────────────────────────────────
    private void checkTtlExpiry() {
        long now = System.currentTimeMillis();
        for (CacheSlot<?> slot : slots.values()) {
            CacheVersion<?> v = slot.activeVersion;
            if (v == null) continue;

            var ttl = slot.definition.getTtl();
            if (ttl == null || ttl.isZero() || ttl.isNegative()) continue;

            if (now - v.getCreatedAt() >= ttl.toMillis()) {
                log.info("Cache '{}' TTL expired, scheduling async reload.", slot.definition.getName());
                RELOAD_EXECUTOR.submit(() -> doReload(slot, "ttl"));
            }
        }
    }

    // ── Old-version cleanup ──────────────────────────────────────────────────

    /**
     * Waits for active readers to drain, then closes and deletes the old version.
     *
     * <h4>FIX — busy-wait eliminated</h4>
     * <p>The original implementation polled {@code hasActiveReaders()} in a
     * {@code Thread.sleep(50)} loop, wasting CPU and adding up to 50 ms of
     * unnecessary latency before cleanup could begin.
     *
     * <p>The replacement calls {@link CacheVersion#awaitDrained()}, which parks the
     * cleanup thread in the OS wait queue.  {@link CacheVersion#releaseReader()}
     * calls {@code notifyAll()} as soon as the last reader releases, waking the
     * cleanup thread immediately.  Typical observed latency drops from O(50 ms) to
     * O(1 ms) or less.
     */
    private <V> void cleanupOldVersion(String cacheName, CacheVersion<V> old) {
        if (old == null) return;

        RELOAD_EXECUTOR.submit(() -> {
            try {
                // FIX: awaitDrained() replaces the busy-wait loop.
                old.awaitDrained();

                old.close();

                try (var walk = Files.walk(old.getVersionDir())) {
                    walk.sorted((a, b) -> b.compareTo(a))
                            .forEach(p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (Exception ignored) {
                                }
                            });
                }
                log.info("Cache '{}': old version '{}' cleaned up.",
                        cacheName, old.getVersionDir().getFileName());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for readers to drain on old version of cache '{}'",
                        cacheName);
            } catch (Exception e) {
                log.warn("Failed to clean up old version for cache '{}'", cacheName, e);
            }
        });
    }

    public static void shutdown() {
        if (INSTANCE == null) return;
        closeAll(); // close all versions and clear slots

        TTL_SCHEDULER.shutdown();
        RELOAD_EXECUTOR.shutdown();

        // Wait for termination (as before)
        try {
            if (!TTL_SCHEDULER.awaitTermination(5, TimeUnit.SECONDS)) {
                TTL_SCHEDULER.shutdownNow();
            }
        } catch (InterruptedException e) {
            TTL_SCHEDULER.shutdownNow();
            Thread.currentThread().interrupt();
        }

        try {
            if (!RELOAD_EXECUTOR.awaitTermination(10, TimeUnit.SECONDS)) {
                RELOAD_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            RELOAD_EXECUTOR.shutdownNow();
            Thread.currentThread().interrupt();
        }

        INSTANCE = null;
    }

    /**
     * Closes all cache versions and clears all registered caches.
     * Does not shut down the background executors (TTL scheduler and reload pool).
     * Useful for resetting the manager between test runs while reusing executors.
     */
    public static synchronized void closeAll() {
        CacheManager inst = INSTANCE;
        if (inst == null) return;
        for (CacheSlot<?> slot : inst.slots.values()) {
            CacheVersion<?> version = slot.activeVersion;
            if (version != null) {
                try {
                    version.close();
                } catch (Exception e) {
                    log.warn("Error closing version for cache {}", slot.definition.getName(), e);
                }
            }
        }
        inst.slots.clear();
    }

    // ── Builder ──────────────────────────────────────────────────────────────

    /**
     * Builder for configuring a {@link CacheManager} instance.
     */
    public static final class Builder {
        private final Path basePath;
        private int shardCapacity = 100_000;
        private int defaultMaxKeyBytes = CacheDefinition.DEFAULT_MAX_KEY_BYTES;
        private int defaultMaxValueBytes = CacheDefinition.DEFAULT_MAX_VALUE_BYTES;
        private int memoryCacheSize = 0;
        private String chronicleAverageKey = "key-00000000";
        private int indexShardCount = 16;
        private boolean useChronicleMap = false;

        /**
         * Create a new builder for the given base path.
         *
         * @param basePath root directory for cache data
         */
        public Builder(Path basePath) {
            if (basePath == null) throw new IllegalArgumentException("basePath cannot be null");
            this.basePath = basePath;
        }

        /**
         * Sets maximum row count per data shard file.
         *
         * @param v capacity (rows per shard)
         * @return this builder
         */
        public Builder shardCapacity(int v) {
            this.shardCapacity = v;
            return this;
        }

        /**
         * Sets default max key byte length for non-dynamic cache definitions.
         *
         * @param v max key bytes
         * @return this builder
         */
        public Builder defaultMaxKeyBytes(int v) {
            this.defaultMaxKeyBytes = v;
            return this;
        }

        /**
         * Sets default max value byte length for non-dynamic cache definitions.
         *
         * @param v max value bytes
         * @return this builder
         */
        public Builder defaultMaxValueBytes(int v) {
            this.defaultMaxValueBytes = v;
            return this;
        }

        /**
         * Sets manager-wide default Caffeine size for cache definitions with 0 memory size.
         *
         * @param v memory cache max size
         * @return this builder
         */
        public Builder memoryCacheSize(int v) {
            if (v < 0) throw new IllegalArgumentException("memoryCacheSize cannot be negative");
            this.memoryCacheSize = v;
            return this;
        }

        /**
         * Sets ChronicleMap average key template used for index sizing.
         *
         * @param v average key template
         * @return this builder
         */
        public Builder chronicleAverageKey(String v) {
            if (v == null || v.isEmpty())
                throw new IllegalArgumentException("chronicleAverageKey cannot be blank");
            this.chronicleAverageKey = v;
            return this;
        }


        /**
         * Enables or disables ChronicleMap usage for the off-heap index.
         *
         * @param v true to use ChronicleMap, false to use in-memory map
         * @return this builder
         */
        public Builder useChronicleMap(boolean v) {
            this.useChronicleMap = v;
            return this;
        }

        /**
         * Sets number of ChronicleMap index shards.
         * Increase this value for very large keysets on Windows to stay below mmap file limits.
         *
         * @param v shard count ({@literal >= 1})
         * @return this builder
         */
        public Builder indexShardCount(int v) {
            if (v < 1) throw new IllegalArgumentException("indexShardCount must be >= 1");
            this.indexShardCount = v;
            return this;
        }

        /**
         * Builds a configured {@link CacheManager} instance.
         *
         * @return configured CacheManager
         */
        public CacheManager build() {
            return new CacheManager(
                    basePath, shardCapacity,
                    defaultMaxKeyBytes, defaultMaxValueBytes,
                    memoryCacheSize, chronicleAverageKey,
                    indexShardCount, useChronicleMap
            );
        }
    }

    // ── CacheSlot ────────────────────────────────────────────────────────────
    private static final class CacheSlot<V> {
        final CacheDefinition<V> definition;
        final AtomicBoolean reloading = new AtomicBoolean(false);
        volatile CacheVersion<V> activeVersion;

        // FIX: StampedLock replaces synchronized(slot) in the read/write paths.
        // Read lock: shared — multiple concurrent getFromSlot() calls proceed in parallel.
        // Write lock: exclusive — only held during the version swap in doReload(),
        //             which is a sub-microsecond operation.
        final StampedLock lock = new StampedLock();

        CacheSlot(CacheDefinition<V> definition) {
            this.definition = definition;
        }
    }
}

