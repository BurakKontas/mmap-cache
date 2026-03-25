package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CacheCoverageFinalPushTests {

    @Test
    void builder_and_generated_toString_exercised() {
        var b = CacheDefinition.<String>builder();
        b.name("x");
        b.supplier(() -> java.util.stream.Stream.empty());
        b.keyExtractor(CacheRow::getKey);
        b.serializer(CacheDefinition.defaultSerializer());
        b.deserializer(CacheDefinition.defaultDeserializer(s -> s));
        b.ttl(Duration.ZERO);
        b.dynamicSizing(true);
        b.maxKeyBytes(10);
        b.maxValueBytes(20);

        // call builder toString to exercise generated code
        assertNotNull(b.toString());

        CacheDefinition<String> def = b.build();
        assertNotNull(def);
        assertTrue(def.recordSize() > 0);
        assertTrue(def.recordSize(8, 8) > 0);
    }

    @Test
    void cacheEntry_timestamp_and_getters() {
        byte[] v = "abc".getBytes();
        CacheEntry e = new CacheEntry(123L, 456L, "k", v, 8, 8);
        assertEquals(456L, e.getTimestamp());
        assertEquals(123L, e.getId());
        assertNotNull(e.getKey());
    }

    @Test
    void cacheRow_tableName_and_fetchedAt() {
        CacheRow r1 = new CacheRow("tbl", "k", "v");
        assertEquals("tbl", r1.getTableName());
        assertTrue(r1.getFetchedAt() > 0);

        CacheRow r2 = new CacheRow("k", null);
        assertNull(r2.getValueAsString());
    }

    @Test
    void cacheVersion_getters_covered() throws Exception {
        Path tmp = Files.createTempDirectory("cache_final_push");
        CacheVersion<String> v = new CacheVersion<>(tmp, new CacheShard[0], Map.of(), 5);
        assertEquals(5, v.getMemoryCacheSize());
        assertNotNull(v.getReaderCount());
        assertNotNull(v.getVersionDir());
    }
}
