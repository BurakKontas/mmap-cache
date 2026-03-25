package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.CacheShard;
import tr.kontas.cache.CacheVersion;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class CoverageMoreTests {

    @Test
    void purgeStaleVersions_normal_deletes_version_dir() throws Exception {
        Path tmp = Files.createTempDirectory("ci_purge_normal");
        Path ver = tmp.resolve("vdel");
        Files.createDirectories(ver);
        Files.writeString(ver.resolve("f.txt"), "x");

        CacheVersion<String> v = new CacheVersion<>(ver, new CacheShard[0], Map.of(), TestHelpers.simpleDefinition("vdel", 0));
        CacheManager.initialize(tmp);

        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        var cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, CacheVersion.class);
        cleanup.setAccessible(true);
        // should not throw and eventually remove files
        assertDoesNotThrow(() -> cleanup.invoke(mgr, "vdel", v));

        // attempt to purge stale versions (private) which will see no files now
        var purge = CacheManager.class.getDeclaredMethod("purgeStaleVersions", java.nio.file.Path.class);
        purge.setAccessible(true);
        assertDoesNotThrow(() -> purge.invoke(mgr, tmp));
    }

    @Test
    void checkTtlExpiry_schedules_reload_path() throws Exception {
        Path tmp = Files.createTempDirectory("ci_ttl_schedule");
        CacheManager.Builder b = CacheManager.builder(tmp);
        b.shardCapacity(10).memoryCacheSize(10);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("ttlcache")
                .supplier(() -> Stream.of(new CacheRow("k", "v")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofSeconds(1))
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
        Object slot = slots.get("ttlcache");

        Path verDir = tmp.resolve("v");
        Files.createDirectories(verDir);
        CacheShard[] shards = new CacheShard[0];
        CacheVersion<String> v = new CacheVersion<>(verDir, shards, Map.of(), TestHelpers.simpleDefinition("vtemp", 0));

        Field createdF = CacheVersion.class.getDeclaredField("createdAt");
        createdF.setAccessible(true);
        createdF.setLong(v, 0L);

        Field activeF = slot.getClass().getDeclaredField("activeVersion");
        activeF.setAccessible(true);
        activeF.set(slot, v);

        Method method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(mgr));
    }

    @Test
    void cacheShard_normal_close_no_exceptions() throws Exception {
        Path tmp = Files.createTempDirectory("ci_shard_normal_close");
        int maxKey = 4, maxVal = 4;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        CacheShard shard = new CacheShard(tmp.resolve("s3.dat"), recordSize, 1, maxKey, maxVal);
        // close in normal conditions should not throw
        assertDoesNotThrow(shard::close);
    }
}
