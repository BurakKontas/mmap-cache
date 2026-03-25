package tr.kontas.cache;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public final class CacheManager {
    // manager-wide defaults (configured via Builder)
    private final int shardCapacity;
    private final int memoryCacheSize;
    private final int defaultMaxKeyBytes;
    private final int defaultMaxValueBytes;

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

    private static volatile CacheManager INSTANCE;
    private final ConcurrentHashMap<String, CacheSlot<?>> slots = new ConcurrentHashMap<>();
    private final Path basePath;

    private CacheManager(Path basePath, int shardCapacity, int memoryCacheSize, int defaultMaxKeyBytes, int defaultMaxValueBytes) {
        this.basePath = basePath;
        this.shardCapacity = shardCapacity;
        this.memoryCacheSize = memoryCacheSize;
        this.defaultMaxKeyBytes = defaultMaxKeyBytes;
        this.defaultMaxValueBytes = defaultMaxValueBytes;

        TTL_SCHEDULER.scheduleWithFixedDelay(
                this::checkTtlExpiry,
                10, 10, TimeUnit.SECONDS
        );
    }

    /**
     * Convenience initializer with sensible defaults. If you need custom shard/memory/default sizes,
     * use the {@link Builder} and {@link #initialize(Builder)} instead.
     */
    public static synchronized void initialize(Path basePath) {
        // defaults: shardCapacity=1000, memoryCacheSize=500, default key/value as in CacheDefinition
        INSTANCE = new CacheManager(basePath, 1000, 500, CacheDefinition.DEFAULT_MAX_KEY_BYTES, CacheDefinition.DEFAULT_MAX_VALUE_BYTES);
        purgeStaleVersions(basePath);
    }

    public static synchronized void initialize(File baseDir) {
        if (baseDir == null) throw new IllegalArgumentException("baseDir cannot be null");
        initialize(baseDir.toPath());
    }

    /** Initialize via builder (preferred when you want to customize capacities/defaults). */
    public static synchronized void initialize(Builder builder) {
        if (builder == null) throw new IllegalArgumentException("builder cannot be null");
        INSTANCE = builder.build();
        purgeStaleVersions(INSTANCE.basePath);
    }

    public static Builder builder(Path basePath) {
        return new Builder(basePath);
    }

    public static final class Builder {
        private final Path basePath;
        private int shardCapacity = 1000;
        private int memoryCacheSize = 500;
        private int defaultMaxKeyBytes = CacheDefinition.DEFAULT_MAX_KEY_BYTES;
        private int defaultMaxValueBytes = CacheDefinition.DEFAULT_MAX_VALUE_BYTES;

        public Builder(Path basePath) {
            if (basePath == null) throw new IllegalArgumentException("basePath cannot be null");
            this.basePath = basePath;
        }

        public Builder shardCapacity(int shardCapacity) {
            this.shardCapacity = shardCapacity;
            return this;
        }

        public Builder memoryCacheSize(int memoryCacheSize) {
            this.memoryCacheSize = memoryCacheSize;
            return this;
        }

        public Builder defaultMaxKeyBytes(int bytes) {
            this.defaultMaxKeyBytes = bytes;
            return this;
        }

        public Builder defaultMaxValueBytes(int bytes) {
            this.defaultMaxValueBytes = bytes;
            return this;
        }

        public CacheManager build() {
            return new CacheManager(basePath, shardCapacity, memoryCacheSize, defaultMaxKeyBytes, defaultMaxValueBytes);
        }
    }

    public static <V> void register(CacheDefinition<V> definition) {
        CacheManager mgr = requireInstance();

        // apply manager-wide defaults only when the definition hasn't overridden them
        CacheDefinition<V> effective = definition;
        if (!definition.isDynamicSizing()) {
            CacheDefinition.CacheDefinitionBuilder<V> b = definition.toBuilder();
            if (definition.getMaxKeyBytes() == CacheDefinition.DEFAULT_MAX_KEY_BYTES && mgr.defaultMaxKeyBytes != CacheDefinition.DEFAULT_MAX_KEY_BYTES) {
                b.maxKeyBytes(mgr.defaultMaxKeyBytes);
            }
            if (definition.getMaxValueBytes() == CacheDefinition.DEFAULT_MAX_VALUE_BYTES && mgr.defaultMaxValueBytes != CacheDefinition.DEFAULT_MAX_VALUE_BYTES) {
                b.maxValueBytes(mgr.defaultMaxValueBytes);
            }
            effective = b.build();
        }

        CacheSlot<V> slot = new CacheSlot<>(effective);
        mgr.slots.put(effective.getName(), slot);
        mgr.doReload(slot, "init");
        log.info("Cache '{}' registered and loaded.", effective.getName());
    }

    @SuppressWarnings("unchecked")
    public static <V> V get(String cacheName, String key) {
        CacheSlot<V> slot = (CacheSlot<V>) requireInstance().slots.get(cacheName);
        if (slot == null) {
            log.warn("Cache '{}' not registered.", cacheName);
            return null;
        }
        return getFromSlot(slot, key);
    }

    @SuppressWarnings("unchecked")
    public static void reload(String cacheName) {
        CacheSlot<?> slot = requireInstance().slots.get(cacheName);
        if (slot == null) throw new IllegalArgumentException("Unknown cache: " + cacheName);
        requireInstance().doReload((CacheSlot<Object>) slot, "manual");
    }

    public static void reloadAsync(String cacheName) {
        RELOAD_EXECUTOR.submit(() -> reload(cacheName));
    }

    private static <V> V getFromSlot(CacheSlot<V> slot, String key) {
        CacheVersion<V> version = slot.activeVersion;
        if (version == null) return null;

        V cached = version.getMemoryCache().get(key);
        if (cached != null) return cached;

        version.acquireReader();
        try {
            CacheLocation loc = version.getIndex().get(key);
            if (loc == null) return null;

            CacheEntry entry = version.getShards()[loc.shardId()].read(loc.offset());
            V value = slot.definition.getDeserializer().apply(entry.getValueBytes());

            version.getMemoryCache().put(key, value);
            return value;
        } finally {
            version.releaseReader();
        }
    }

    private static void purgeStaleVersions(Path basePath) {
        if (!Files.exists(basePath)) return;

        try {
            Files.list(basePath).forEach(cacheDir -> {
                if (!Files.isDirectory(cacheDir)) return;

                try {
                    Files.list(cacheDir)
                            .filter(Files::isDirectory)
                            .filter(p -> p.getFileName().toString().startsWith("v"))
                            .forEach(versionDir -> {
                                try {
                                    Files.walk(versionDir)
                                            .sorted((a, b) -> b.compareTo(a))
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

    private <V> void doReload(CacheSlot<V> slot, String trigger) {
        if (!slot.reloading.compareAndSet(false, true)) {
            log.info("Reload already in progress for cache '{}'", slot.definition.getName());
            return;
        }

        String name = slot.definition.getName();

        boolean success = false;

        try {
            Path cacheDir = basePath.resolve(name);
            Files.createDirectories(cacheDir);

            long versionTs = System.currentTimeMillis();
            Path versionDir = cacheDir.resolve("v" + versionTs);
            Files.createDirectories(versionDir);

            List<CacheRow> rows;
            try (var stream = slot.definition.getSupplier().get()) {
                rows = stream.collect(Collectors.toList());
            }

            int total = rows.size();

            // --- sizing ---
            final int maxKeyBytes;
            final int maxValueBytes;
            if (slot.definition.isDynamicSizing() && total > 0) {
                int maxK = 0, maxV = 0;
                for (CacheRow row : rows) {
                    String k = slot.definition.getKeyExtractor().apply(row);
                    byte[] v = slot.definition.getSerializer().apply(row);
                    maxK = Math.max(maxK, k.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
                    maxV = Math.max(maxV, v.length);
                }
                // add 25% headroom so small additions don't force a full schema change
                maxKeyBytes = (int) (maxK * 1.25) + 1;
                maxValueBytes = (int) (maxV * 1.25) + 1;
                log.info("Cache '{}': dynamic sizing → maxKeyBytes={}, maxValueBytes={}", name, maxKeyBytes, maxValueBytes);
            } else {
                maxKeyBytes = slot.definition.getMaxKeyBytes();
                maxValueBytes = slot.definition.getMaxValueBytes();
            }
            int recordSize = slot.definition.recordSize(maxKeyBytes, maxValueBytes);
            // --- /sizing ---

            int shardCount = Math.max(1, (int) Math.ceil((double) total / shardCapacity));

            log.info("Cache '{}': {} rows → {} shard(s)", name, total, shardCount);

            CacheShard[] newShards = new CacheShard[shardCount];
            for (int i = 0; i < shardCount; i++) {
                int shardSize = (i < shardCount - 1)
                        ? shardCapacity
                        : total - (i * shardCapacity);

                Path shardPath = versionDir.resolve(String.format("shard_%04d.dat", i));
                newShards[i] = new CacheShard(shardPath, recordSize, shardSize, maxKeyBytes, maxValueBytes);
            }

            Map<String, CacheLocation> newIndex = new HashMap<>((int) (total * 1.3));

            for (int i = 0; i < total; i++) {
                CacheRow row = rows.get(i);
                int shardId = i / shardCapacity;
                int offset = i % shardCapacity;

                String key = slot.definition.getKeyExtractor().apply(row);
                byte[] valueBytes = slot.definition.getSerializer().apply(row);

                CacheEntry entry = new CacheEntry(i + 1L, versionTs, key, valueBytes, maxKeyBytes, maxValueBytes);

                boolean written = newShards[shardId].write(offset, entry);
                if (!written) {
                    log.warn("Cache '{}': skipping entry for key '{}' (value too large)", name, key);
                    continue;
                }
                newIndex.put(key, new CacheLocation(shardId, offset));
            }

            for (CacheShard shard : newShards) shard.flush();

            CacheVersion<V> newVersion = new CacheVersion<>(
                    versionDir,
                    newShards,
                    Collections.unmodifiableMap(newIndex),
                    memoryCacheSize
            );

            CacheVersion<V> oldVersion = slot.activeVersion;
            slot.activeVersion = newVersion;


            log.info("Cache '{}' reloaded. version={}, shards={}, entries={}",
                    name, versionDir.getFileName(), shardCount, total);

            cleanupOldVersion(name, oldVersion, versionDir);
            success = true;

        } catch (Exception e) {
            log.error("Reload failed for cache '{}'", name, e);
            throw new RuntimeException("Reload failed for cache: " + name, e);
        } finally {
            slot.reloading.set(false);
        }
    }

    private void checkTtlExpiry() {
        long now = System.currentTimeMillis();
        for (CacheSlot<?> slot : slots.values()) {
            CacheVersion<?> v = slot.activeVersion;
            if (v == null) continue;

            long ttlMs = slot.definition.getTtl().toMillis();
            if (ttlMs <= 0) continue;

            if (now - v.getCreatedAt() >= ttlMs) {
                log.info("Cache '{}' TTL expired, scheduling async reload.", slot.definition.getName());
                RELOAD_EXECUTOR.submit(() -> doReload(slot, "ttl"));
            }
        }
    }

    private <V> void cleanupOldVersion(String cacheName, CacheVersion<V> old, Path currentVersionDir) {
        if (old == null) return;

        new Thread(() -> {
            try {
                while (old.hasActiveReaders()) Thread.sleep(50);

                old.close();

                Files.walk(old.getVersionDir())
                        .sorted((a, b) -> b.compareTo(a))
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (Exception ignored) {
                            }
                        });

                log.info("Cache '{}': old version '{}' cleaned up.",
                        cacheName, old.getVersionDir().getFileName());

            } catch (Exception e) {
                log.warn("Failed to clean up old version for cache '{}'", cacheName, e);
            }
        }, "cache-cleanup-" + cacheName).start();
    }

    private static final class CacheSlot<V> {
        final CacheDefinition<V> definition;
        final AtomicBoolean reloading = new AtomicBoolean(false);
        volatile CacheVersion<V> activeVersion;

        CacheSlot(CacheDefinition<V> definition) {
            this.definition = definition;
        }
    }
}

