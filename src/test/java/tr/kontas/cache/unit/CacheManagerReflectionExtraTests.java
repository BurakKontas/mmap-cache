package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.*;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class CacheManagerReflectionExtraTests {

    @Test
    void dynamicSizing_emptySupplier_doesNotThrow_and_getReturnsNull() throws Exception {
        Path tmp = Files.createTempDirectory("cache_dyn_empty");
        CacheManager.Builder b = CacheManager.builder(tmp);
        b.shardCapacity(10).memoryCacheSize(10);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("dynempty")
                .supplier(Stream::empty)
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofSeconds(60))
                .dynamicSizing(true)
                .build();

        assertDoesNotThrow(() -> CacheManager.register(def));
        assertNull(CacheManager.get("dynempty", "any"));
    }

    @Test
    void getFromSlot_whenLocNull_returnsNull() throws Exception {
        Path tmp = Files.createTempDirectory("cache_get_loc_null");
        CacheManager.Builder b = CacheManager.builder(tmp);
        b.shardCapacity(10).memoryCacheSize(10);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("locnull")
                .supplier(() -> Stream.of(new CacheRow("k1", "v1")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofSeconds(60))
                .build();
        CacheManager.register(def);

        // get slot via reflection
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);
        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentHashMap<String, Object> slots = (java.util.concurrent.ConcurrentHashMap<String, Object>) slotsF.get(inst);
        Object slot = slots.get("locnull");

        // create a version with empty index and assign
        Path verDir = tmp.resolve("v");
        Files.createDirectories(verDir);
        CacheShard[] shards = new CacheShard[0];
        CacheVersion<String> version = new CacheVersion<>(verDir, shards, Map.of(), TestHelpers.simpleDefinition("vloc", 0));

        Field activeF = slot.getClass().getDeclaredField("activeVersion");
        activeF.setAccessible(true);
        activeF.set(slot, version);

        assertNull(CacheManager.get("locnull", "nope"));
    }

    @Test
    void cacheShard_flushAndClose_handleNullBufferAndChannel() throws Exception {
        Path tmp = Files.createTempDirectory("cache_shard_nulls");
        int maxKey = 8, maxVal = 8;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        CacheShard shard = new CacheShard(tmp.resolve("s.dat"), recordSize, 1, maxKey, maxVal);

        // set buffer to null to cause NPE inside flush() which should be caught
        java.lang.reflect.Field bufF = CacheShard.class.getDeclaredField("buffer");
        bufF.setAccessible(true);
        bufF.set(shard, null);

        // set channel to null to cause NPE in close() path
        java.lang.reflect.Field chF = CacheShard.class.getDeclaredField("channel");
        chF.setAccessible(true);
        chF.set(shard, null);

        // should not throw
        assertDoesNotThrow(shard::flush);
        assertDoesNotThrow(shard::close);
    }

    @Test
    void requireInstance_throwsWhenNotInitialized() throws Exception {
        // set INSTANCE to null and verify IllegalStateException from get
        java.lang.reflect.Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        instF.set(null, null);

        assertThrows(IllegalStateException.class, () -> CacheManager.get("nope", "k"));
    }
}
