package tr.kontas.cache;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Getter
public final class CacheVersion<V> {
    private static final int DEFAULT_MEMORY_CACHE_SIZE = 0;
    private final int memoryCacheSize;
    private final Path versionDir;
    private final CacheShard[] shards;
    private final Map<String, CacheLocation> index;
    private final long createdAt;
    private final Map<String, V> memoryCache = Collections.synchronizedMap(
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, V> eldest) {
                    return size() > memoryCacheSize;
                }
            }
    );

    private final AtomicInteger readerCount = new AtomicInteger();

    public CacheVersion(
            Path versionDir,
            CacheShard[] shards,
            Map<String, CacheLocation> index
    ) {
        this(versionDir, shards, index, DEFAULT_MEMORY_CACHE_SIZE);
    }

    public CacheVersion(
            Path versionDir,
            CacheShard[] shards,
            Map<String, CacheLocation> index,
            int memoryCacheSize
    ) {
        this.versionDir = versionDir;
        this.shards = shards;
        this.index = index;
        this.createdAt = System.currentTimeMillis();
        this.memoryCacheSize = memoryCacheSize;
    }

    public void acquireReader() {
        readerCount.incrementAndGet();
    }

    public void releaseReader() {
        readerCount.decrementAndGet();
    }

    public boolean hasActiveReaders() {
        return readerCount.get() > 0;
    }

    public void close() {
        for (CacheShard shard : shards) {
            try {
                shard.close();
            } catch (Exception e) {
                log.warn("Failed to close shard", e);
            }
        }
    }
}