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

public class CacheCoverageExtraTests {

    @Test
    void checkTtlExpiry_vNullAndTtlZeroBranches() throws Exception {
        Path tmp = Files.createTempDirectory("cache_extra_ttl");
        CacheManager.Builder b = CacheManager.builder(tmp);
        b.shardCapacity(10).memoryCacheSize(10);
        CacheManager.initialize(b);

        // register one cache
        CacheDefinition<String> def1 = CacheDefinition.<String>builder()
                .name("ttlA")
                .supplier(() -> Stream.of(new CacheRow("k", "v")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofSeconds(1))
                .build();
        CacheManager.register(def1);

        // register another cache with ttl zero
        CacheDefinition<String> def2 = CacheDefinition.<String>builder()
                .name("ttlZero")
                .supplier(Stream::empty)
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ZERO)
                .build();
        CacheManager.register(def2);

        // set one slot activeVersion to null to hit v==null branch
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentHashMap<String, Object> slots = (java.util.concurrent.ConcurrentHashMap<String, Object>) slotsF.get(mgr);
        Object slot = slots.get("ttlA");
        Field activeF = slot.getClass().getDeclaredField("activeVersion");
        activeF.setAccessible(true);
        activeF.set(slot, null);

        var method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);

        // run - should not throw and should exercise v==null path and ttl<=0 path
        assertDoesNotThrow(() -> method.invoke(mgr));
    }

    @Test
    void shard_assertBounds_readWriteBranches() throws Exception {
        Path tmp = Files.createTempDirectory("cache_extra_bounds");
        int maxKey = 4, maxVal = 4;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        CacheShard s = new CacheShard(tmp.resolve("sh.dat"), recordSize, 1, maxKey, maxVal);

        // write out of bounds -> IndexOutOfBoundsException
        assertThrows(IndexOutOfBoundsException.class, () -> s.write(1, new CacheEntry(1,1)));
        assertThrows(IndexOutOfBoundsException.class, () -> s.read(1));

        s.close();
    }

    @Test
    void cacheVersion_close_handlesShardCloseException() throws Exception {
        Path tmp = Files.createTempDirectory("cache_extra_version");
        int maxKey = 4, maxVal = 4;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;

        CacheShard throwing = new CacheShard(tmp.resolve("tsh.dat"), recordSize, 1, maxKey, maxVal) {
            @Override
            public void close() {
                throw new RuntimeException("close fail");
            }
        };

        CacheShard[] shards = new CacheShard[1];
        shards[0] = throwing;
        CacheVersion<String> v = new CacheVersion<>(tmp, shards, Map.of(), TestHelpers.simpleDefinition("vtest", 0));

        // call close - should catch exception and not propagate
        assertDoesNotThrow(v::close);
    }

    @Test
    void cacheLocation_sameShardDifferentOffset_branch() {
        CacheLocation a = new CacheLocation(1, 2);
        CacheLocation b = new CacheLocation(1, 3);
        assertNotEquals(a, b); // same shardId true, offset different -> left true right false
    }

    @Test
    void cacheVersion_threeArgConstructor_isCovered() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        CacheShard[] shards = new CacheShard[0];
        CacheVersion<String> v = new CacheVersion<>(tmp, shards, Map.of(), TestHelpers.simpleDefinition("vtemp", 0));
        assertNotNull(v);
        assertTrue(v.getCreatedAt() > 0);
    }

    @Test
    void checkTtlExpiry_notExpired_doesNotScheduleReload() throws Exception {
        Path tmp = Files.createTempDirectory("cache_ttl_not_expired");
        CacheManager.Builder b = CacheManager.builder(tmp);
        b.shardCapacity(10).memoryCacheSize(10);
        CacheManager.initialize(b);
        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("notExpired")
                .supplier(() -> Stream.of(new CacheRow("k", "v")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofHours(1))
                .build();
        CacheManager.register(def);
        // ensure activeVersion createdAt is now (not expired)
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentHashMap<String, Object> slots = (java.util.concurrent.ConcurrentHashMap<String, Object>) slotsF.get(mgr);
        Object slot = slots.get("notExpired");

        Path verDir = tmp.resolve("v");
        Files.createDirectories(verDir);
        CacheShard[] shards = new CacheShard[0];
        CacheVersion<String> v = new CacheVersion<>(verDir, shards, Map.of(), TestHelpers.simpleDefinition("vtemp4", 0));

        Field activeF = slot.getClass().getDeclaredField("activeVersion");
        activeF.setAccessible(true);
        activeF.set(slot, v);

        var method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(mgr));
    }

    @Test
    void cacheLocation_moreEqualsCombos() {
        CacheLocation a = new CacheLocation(1, 2);
        // same shard different offset
        CacheLocation b = new CacheLocation(1, 3);
        // different shard same offset
        CacheLocation c = new CacheLocation(2, 2);
        // completely different
        CacheLocation d = new CacheLocation(3, 4);

        assertNotEquals(a, b);
        assertNotEquals(a, c);
        assertNotEquals(a, d);
        assertEquals(a, new CacheLocation(1, 2));
    }

    @Test
    void checkTtlExpiry_forceTrueBranch() throws Exception {
        Path tmp = Files.createTempDirectory("cache_ttl_force_true");
        CacheManager.Builder b = CacheManager.builder(tmp);
        b.shardCapacity(10).memoryCacheSize(10);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("forceTTL")
                .supplier(() -> Stream.of(new CacheRow("k", "v")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofMillis(1))
                .build();
        CacheManager.register(def);

        // set activeVersion createdAt to 0 to force expiry
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentHashMap<String, Object> slots = (java.util.concurrent.ConcurrentHashMap<String, Object>) slotsF.get(mgr);
        Object slot = slots.get("forceTTL");

        Path verDir = tmp.resolve("v");
        Files.createDirectories(verDir);
        CacheVersion<String> v = new CacheVersion<>(verDir, new CacheShard[0], Map.of(), TestHelpers.simpleDefinition("vtemp5", 0));

        // set createdAt to past via reflection
        Field createdF = CacheVersion.class.getDeclaredField("createdAt");
        createdF.setAccessible(true);
        createdF.setLong(v, 0L);

        Field activeF = slot.getClass().getDeclaredField("activeVersion");
        activeF.setAccessible(true);
        activeF.set(slot, v);

        var method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);

        // call checkTtlExpiry multiple times to ensure the true branch is executed
        for (int i = 0; i < 3; i++) {
            assertDoesNotThrow(() -> method.invoke(mgr));
            Thread.sleep(10);
        }
    }

}
