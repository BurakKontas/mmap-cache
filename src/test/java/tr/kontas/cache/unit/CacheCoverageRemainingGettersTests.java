package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CacheCoverageRemainingGettersTests {

    @Test
    void exercise_lombok_getters_and_toString() throws Exception {
        // CacheDefinition toString
        CacheDefinition<?> def = CacheDefinition.builder()
                .name("d")
                .supplier(() -> java.util.stream.Stream.empty())
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(java.time.Duration.ZERO)
                .build();
        assertNotNull(def.toString());

        // CacheEntry getters
        byte[] v = "x".getBytes();
        CacheEntry e = new CacheEntry(1L, 2L, "k", v, 8, 8);
        assertEquals(1L, e.getId());
        assertEquals(2L, e.getTimestamp());
        assertEquals("k", e.getKey());
        assertArrayEquals(v, e.getValueBytes());

        // CacheRow getters
        CacheRow r = new CacheRow("t", "kk", "vv");
        assertEquals("kk", r.getKey());
        assertEquals("vv", r.getValueAsString());
        assertNotNull(r.getFetchedAt());
        assertNotNull(r.toString());

        // CacheShard getFilePath
        Path tmp = Files.createTempDirectory("cache_remain");
        int maxKey = 4, maxVal = 4;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        CacheShard shard = new CacheShard(tmp.resolve("s.dat"), recordSize, 1, maxKey, maxVal);
        assertNotNull(shard.getFilePath());
        shard.close();

        // CacheVersion getters
        CacheVersion<String> vrs = new CacheVersion<>(tmp, new CacheShard[0], Map.of(), TestHelpers.simpleDefinition("cv2", 2));
        // memoryCacheMaxSize was set to 2 so memoryCache should exist
        assertNotNull(vrs.getMemoryCache());
        assertNotNull(vrs.getReaderCount());
    }
}
