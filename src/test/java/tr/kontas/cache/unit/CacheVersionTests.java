package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheVersion;
import tr.kontas.cache.CacheShard;
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.CacheLocation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class CacheVersionTests {

    @Test
    void awaitDrained_and_readerCounting_works() throws Exception {
        Path tmp = Files.createTempDirectory("cvtest");
        CacheShard shard = new CacheShard(tmp.resolve("s.dat"), 64, 1, 10, 10);
        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("vtest")
                .supplier(Stream::empty)
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .build();

        CacheVersion<String> v = new CacheVersion<>(tmp, new CacheShard[]{shard}, (Map<String, CacheLocation>[]) null, def);

        assertFalse(v.hasActiveReaders());
        v.acquireReader();
        assertTrue(v.hasActiveReaders());
        v.releaseReader();
        v.awaitDrained();
        assertFalse(v.hasActiveReaders());

        v.close();
        // closing again should not throw
        v.close();
    }
}
