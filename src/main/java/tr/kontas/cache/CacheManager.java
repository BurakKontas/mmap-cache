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

    private static final ExecutorService RELOAD_EXECUTOR =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "cache-reload-worker");
                t.setDaemon(true);
                return t;
            });

    // ── Singleton ────────────────────────────────────────────────────────────
    // volatile ensures the INSTANCE write in initialize() is visible to all
    // threads that subsequently call requireInstance() without synchronization.
    private static volatile CacheManager INSTANCE;

    // ── Manager config ───────────────────────────────────────────────────────
    private final Path basePath;
    private final int shardCapacity;
    private final int defaultMaxKeyBytes;
    private final int defaultMaxValueBytes;
    private final int memoryCacheSize;
    private final String chronicleAverageKey;
    private final int indexShardCount;

    private final ConcurrentHashMap<String, CacheSlot<?>> slots = new ConcurrentHashMap<>();

    // ── Constructor ──────────────────────────────────────────────────────────
    private CacheManager(
            Path basePath,
            int shardCapacity,
            int defaultMaxKeyBytes,
            int defaultMaxValueBytes,
            int memoryCacheSize,
            String chronicleAverageKey,
            int indexShardCount
    ) {
        this.basePath = basePath;
        this.shardCapacity = shardCapacity;
        this.defaultMaxKeyBytes = defaultMaxKeyBytes;
        this.defaultMaxValueBytes = defaultMaxValueBytes;
        this.memoryCacheSize = memoryCacheSize;
        this.chronicleAverageKey = chronicleAverageKey;
        this.indexShardCount = indexShardCount;

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
        // Default shardCapacity raised from 1 000 → 100 000.
        // At 1 000 a 10M-row cache creates 10 000 MappedByteBuffer instances, each
        // with its own page-table entries, blowing OS virtual-memory limits and
        // causing latency spikes on random reads.
        INSTANCE = new CacheManager(
                basePath,
                100_000,
                CacheDefinition.DEFAULT_MAX_KEY_BYTES,
                CacheDefinition.DEFAULT_MAX_VALUE_BYTES,
                0,
                "key-00000000",
                16
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
     * Thread-safe read path.
     *
     * <p>FIX (race condition): the original code did:
     * <pre>
     *   (1) version = slot.activeVersion   // volatile read
     *   (2) version.acquireReader()        // refcount++
     * </pre>
     * Between (1) and (2) another thread could swap {@code slot.activeVersion} to a
     * new version and {@code cleanupOldVersion} could poll {@code hasActiveReaders()},
     * see zero, and immediately close the old version. When (2) then ran on that closed
     * version it would increment a stale counter and the subsequent
     * {@code FallbackCloseableMap.get()} would throw {@code IllegalStateException}.
     *
     * <p>Fix: both the acquire and the swap are guarded by {@code synchronized (slot)},
     * making the handoff window atomic. cleanupOldVersion therefore cannot start polling
     * until after acquireReader() has already been called.
     */
    private static <V> V getFromSlot(CacheSlot<V> slot, String key) {
        CacheVersion<V> version;
        synchronized (slot) {
            version = slot.activeVersion;
            if (version == null) return null;
            version.acquireReader();   // atomic w.r.t. the version swap in doReload
        }
        try {
            // 1. Caffeine in-memory layer
            V cached = version.getFromMemory(key);
            if (cached != null) return cached;

            // Determine index shard safely
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
     * Since we always purge stale versions on startup and delete old version directories
     * after each reload, the index file is always freshly created — so "same version"
     * is always accurate.
     *
     * <h4>Why we must NOT pre-create the file</h4>
     * A zero-byte file has no ChronicleMap header. When {@code createOrRecoverPersistedTo}
     * opens an existing (but empty) file it attempts to read the header; finding nothing,
     * it throws {@link net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException}.
     * We create the parent directory; the file itself is left to ChronicleMap.
     */
    private static Map<String, CacheLocation> buildChronicleIndex(
            Path file, int expectedEntries, String averageKey
    ) throws IOException {
        int entries = Math.max(expectedEntries, 1);

        // Create parent dir only — do NOT pre-create the file.
        if (file.getParent() != null) Files.createDirectories(file.getParent());

        if (!file.toFile().exists()) Files.createFile(file);

        try {
            Map<String, CacheLocation> map = ChronicleMapBuilder
                    .of(String.class, CacheLocation.class)
                    .name("cache-index")
                    .entries(entries)
                    .averageKey(averageKey)
                    .averageKeySize(averageKey.getBytes(StandardCharsets.UTF_8).length)
                    .averageValueSize(64) // CacheLocation is a small object -> 32-128 bytes is sufficient
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
            return new FallbackCloseableMap<>();
        } catch (Throwable t) {
            log.warn("Failed to create ChronicleMap index, falling back to in-memory map: {}",
                    t.toString());
            return new FallbackCloseableMap<>();
        }
    }

    /**
     * Heap-based fallback index used when ChronicleMap is unavailable.
     *
     * <p>Throws {@link IllegalStateException} on any access after {@link #close()},
     * mirroring ChronicleMap's behaviour so callers treat both implementations uniformly.
     */
    private static final class FallbackCloseableMap<K, V>
            extends ConcurrentHashMap<K, V>
            implements java.io.Closeable {

        private volatile boolean closed = false;

        @Override
        public synchronized void close() {
            closed = true;
        }

        private void ensureOpen() {
            if (closed) throw new IllegalStateException("Index map is closed");
        }

        @Override
        public V put(K key, V value) {
            ensureOpen();
            return super.put(key, value);
        }

        @Override
        public V get(Object key) {
            ensureOpen();
            return super.get(key);
        }

        @Override
        public V remove(Object key) {
            ensureOpen();
            return super.remove(key);
        }

        @Override
        public void clear() {
            ensureOpen();
            super.clear();
        }
    }

    // ── Stale version purge ──────────────────────────────────────────────────
    private static void purgeStaleVersions(Path basePath) {
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
            //
            // Case A: when the caller has already provided both the row count (via
            // SizedSupplier) and non-default max-byte bounds (via the definition),
            // a full sizing scan is entirely redundant — skip it. At 50M rows this
            // saves one full stream traversal (~10 s on the benchmark machine).

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
                    // SizedSupplier hint preferred when present (scan count can under-count
                    // if the supplier skips rows on retry).
                    total = (int) knownCount.orElse(count[0]);
                    // 25% headroom so minor future growth doesn't force a full rebuild;
                    // +1 guarantees ≥1 byte when observed max is 0.
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
                        chronicleFile, expectedPerShard, chronicleAverageKey
                );
            }

            // ── Pass-2: write & index ────────────────────────────────────────
            // CacheEntry is reused across rows (no per-row heap allocation).
            // IMPORTANT: safe only because the stream is sequential and
            // CacheShard.write() serialises the entry into the ByteBuffer before
            // returning. If .parallel() is ever added, replace with ThreadLocal<CacheEntry>.
            CacheEntry reusableEntry = new CacheEntry(maxKeyBytes, maxValueBytes);
            int[] counter = {0};
            int[] skipped = {0};

            try (var stream = slot.definition.getSupplier().get()) {
                stream.forEach(row -> {
                    int i = counter[0]++;
                    int shardId = i / shardCapacity;
                    int offset = i % shardCapacity;

                    String key = slot.definition.getKeyExtractor().apply(row);
                    byte[] valueBytes = slot.definition.getSerializer().apply(row);

                    reusableEntry.reset(i + 1L, versionTs, key, valueBytes);

                    boolean written = newShards[shardId].write(offset, reusableEntry);
                    if (!written) {
                        // Slot at (shardId, offset) is burned but unreachable via index —
                        // safe because shard was pre-sized for `total` slots.
                        log.warn("Cache '{}': skipping key '{}' (value too large)", name, key);
                        skipped[0]++;
                        return;
                    }
                    int indexShardId = (key.hashCode() & 0x7fffffff) % indexShardCount;
                    indexShards[indexShardId].put(key, new CacheLocation(shardId, offset));
                });
            }

            if (skipped[0] > 0) {
                log.warn("Cache '{}': {} / {} entries skipped (value exceeded maxValueBytes={}). " +
                                "Consider raising maxValueBytes or enabling dynamicSizing.",
                        name, skipped[0], total, maxValueBytes);
            }

            for (CacheShard shard : newShards) shard.flush();

            CacheVersion<V> newVersion = new CacheVersion<>(
                    versionDir, newShards, indexShards, slot.definition
            );

            // ── Atomic version swap ──────────────────────────────────────────
            // Synchronized on slot so getFromSlot's acquireReader() cannot race with
            // cleanupOldVersion's hasActiveReaders() poll. Both sides hold the slot
            // monitor, making the handoff atomic.
            CacheVersion<V> oldVersion;
            synchronized (slot) {
                oldVersion = slot.activeVersion;
                slot.activeVersion = newVersion;
            }

            log.info("Cache '{}' reloaded. version={}, shards={}, entries={}",
                    name, versionDir.getFileName(), shardCount, total - skipped[0]);

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
    private <V> void cleanupOldVersion(String cacheName, CacheVersion<V> old) {
        if (old == null) return;

        RELOAD_EXECUTOR.submit(() -> {
            try {
                while (old.hasActiveReaders()) Thread.sleep(50);

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
            } catch (Exception e) {
                log.warn("Failed to clean up old version for cache '{}'", cacheName, e);
            }
        });
    }

    // ── Builder ──────────────────────────────────────────────────────────────
    public static final class Builder {
        private final Path basePath;
        private int shardCapacity = 100_000;  // raised from 1 000
        private int defaultMaxKeyBytes = CacheDefinition.DEFAULT_MAX_KEY_BYTES;
        private int defaultMaxValueBytes = CacheDefinition.DEFAULT_MAX_VALUE_BYTES;
        private int memoryCacheSize = 0;
        private String chronicleAverageKey = "key-00000000";
        private int indexShardCount = 16;

        public Builder(Path basePath) {
            if (basePath == null) throw new IllegalArgumentException("basePath cannot be null");
            this.basePath = basePath;
        }

        /**
         * Sets maximum row count per data shard file.
         */
        public Builder shardCapacity(int v) {
            this.shardCapacity = v;
            return this;
        }

        /**
         * Sets default max key byte length for non-dynamic cache definitions.
         */
        public Builder defaultMaxKeyBytes(int v) {
            this.defaultMaxKeyBytes = v;
            return this;
        }

        /**
         * Sets default max value byte length for non-dynamic cache definitions.
         */
        public Builder defaultMaxValueBytes(int v) {
            this.defaultMaxValueBytes = v;
            return this;
        }

        /**
         * Sets manager-wide default Caffeine size for cache definitions with 0 memory size.
         */
        public Builder memoryCacheSize(int v) {
            if (v < 0) throw new IllegalArgumentException("memoryCacheSize cannot be negative");
            this.memoryCacheSize = v;
            return this;
        }

        /**
         * Sets ChronicleMap average key template used for index sizing.
         */
        public Builder chronicleAverageKey(String v) {
            if (v == null || v.isEmpty())
                throw new IllegalArgumentException("chronicleAverageKey cannot be blank");
            this.chronicleAverageKey = v;
            return this;
        }

        /**
         * Sets number of ChronicleMap index shards.
         * Increase this value for very large keysets on Windows to stay below mmap file limits.
         */
        public Builder indexShardCount(int v) {
            if (v < 1) throw new IllegalArgumentException("indexShardCount must be >= 1");
            this.indexShardCount = v;
            return this;
        }

        /**
         * Builds a configured {@link CacheManager} instance.
         */
        public CacheManager build() {
            return new CacheManager(
                    basePath, shardCapacity,
                    defaultMaxKeyBytes, defaultMaxValueBytes,
                    memoryCacheSize, chronicleAverageKey,
                    indexShardCount
            );
        }
    }

    // ── CacheSlot ────────────────────────────────────────────────────────────
    private static final class CacheSlot<V> {
        final CacheDefinition<V> definition;
        final AtomicBoolean reloading = new AtomicBoolean(false);
        volatile CacheVersion<V> activeVersion;

        CacheSlot(CacheDefinition<V> definition) {
            this.definition = definition;
        }
    }
}
