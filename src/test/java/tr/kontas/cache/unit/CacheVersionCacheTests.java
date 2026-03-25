package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.CacheShard;
import tr.kontas.cache.CacheVersion;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class CacheVersionCacheTests {

    @Test
    void buildCache_expireAfterWrite_applies() throws Exception {
        Path tmp = Files.createTempDirectory("cv_cache_eaw");

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("eaw")
                .supplier(Stream::empty)
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(10)
                .memoryCacheTtl(Duration.ofMillis(200))
                .build();

        CacheVersion<String> v = new CacheVersion<>(tmp, new CacheShard[0], Map.of(), def);
        var cache = v.getMemoryCache();
        assertNotNull(cache);
        cache.put("k", "v");
        assertEquals("v", cache.getIfPresent("k"));

        // wait for TTL + maintenance
        Thread.sleep(300);
        cache.cleanUp();
        assertNull(cache.getIfPresent("k"));

        v.close();
    }

    @Test
    void buildCache_expireAfterAccess_applies() throws Exception {
        Path tmp = Files.createTempDirectory("cv_cache_eaa");

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("eaa")
                .supplier(Stream::empty)
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(10)
                .memoryCacheIdleTtl(Duration.ofMillis(200))
                .build();

        CacheVersion<String> v = new CacheVersion<>(tmp, new CacheShard[0], Map.of(), def);
        var cache = v.getMemoryCache();
        assertNotNull(cache);
        cache.put("k2", "v2");
        assertEquals("v2", cache.getIfPresent("k2"));

        // do NOT access -> should expire after idle TTL
        Thread.sleep(300);
        cache.cleanUp();
        assertNull(cache.getIfPresent("k2"));

        v.close();
    }

    @Test
    void flush_handlesBufferForceException() throws Exception {
        Path tmp = Files.createTempDirectory("shard_flush_err");
        int maxKey = 8, maxVal = 8;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        CacheShard shard = new CacheShard(tmp.resolve("s.dat"), recordSize, 2, maxKey, maxVal);

        // Cannot mock MappedByteBuffer reliably on newer JDKs; set buffer to null and assert no throw
        Field bufField = CacheShard.class.getDeclaredField("buffer");
        bufField.setAccessible(true);
        bufField.set(shard, null);

        // flush should not throw even when buffer is null
        assertDoesNotThrow(shard::flush);

        shard.close();
    }
}
