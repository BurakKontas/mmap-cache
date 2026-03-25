package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.*;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CacheCoverageFinalizerTests {

    @Test
    void initialize_null_checks() {
        assertThrows(IllegalArgumentException.class, () -> CacheManager.initialize((File) null));
        assertThrows(IllegalArgumentException.class, () -> CacheManager.initialize((CacheManager.Builder) null));
        assertThrows(IllegalArgumentException.class, () -> new CacheManager.Builder(null));
    }

    @Test
    void checkTtlExpiry_triggersReloadWhenExpired() throws Exception {
        Path tmp = Files.createTempDirectory("cache_ttl_expired");
        CacheManager.Builder b = CacheManager.builder(tmp);
        b.shardCapacity(10).memoryCacheSize(10);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("ttlcache")
                .supplier(() -> java.util.stream.Stream.of(new CacheRow("k", "v")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofMillis(1))
                .build();
        CacheManager.register(def);

        // set activeVersion createdAt into the past so ttl is considered expired
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentHashMap<String, Object> slots = (java.util.concurrent.ConcurrentHashMap<String, Object>) slotsF.get(mgr);
        Object slot = slots.get("ttlcache");

        // create a CacheVersion with createdAt far in the past and assign
        Path verDir = tmp.resolve("v");
        Files.createDirectories(verDir);
        CacheVersion<String> v = new CacheVersion<>(verDir, new CacheShard[0], Map.of(), 0);

        // set createdAt to past via reflection
        Field createdF = CacheVersion.class.getDeclaredField("createdAt");
        createdF.setAccessible(true);
        createdF.setLong(v, 0L);

        Field activeF = slot.getClass().getDeclaredField("activeVersion");
        activeF.setAccessible(true);
        activeF.set(slot, v);

        // call checkTtlExpiry and ensure it doesn't throw (and branch is exercised)
        Method method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(mgr));
    }

    @Test
    void cleanupOldVersion_waitsForReaders_thenCleans() throws Exception {
        Path tmp = Files.createTempDirectory("cache_cleanup_wait");
        Path versionDir = tmp.resolve("vold");
        Files.createDirectories(versionDir);
        Files.write(versionDir.resolve("f"), "x".getBytes());

        CacheVersion<String> v = new CacheVersion<>(versionDir, new CacheShard[0], Map.of(), 0);
        v.acquireReader();

        CacheManager.initialize(tmp);
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Method cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, CacheVersion.class, Path.class);
        cleanup.setAccessible(true);

        // call cleanup; it will start a thread and wait because v has active readers
        cleanup.invoke(mgr, "cname", v, versionDir);

        // after a short delay release reader so cleanup can proceed
        Thread.sleep(200);
        v.releaseReader();

        // wait for cleanup to remove the directory (or timeout)
        long start = System.currentTimeMillis();
        while (Files.exists(versionDir) && System.currentTimeMillis() - start < 5000) {
            Thread.sleep(50);
        }

        assertFalse(Files.exists(versionDir));
    }

    @Test
    void cacheLocation_equalsBranches() {
        CacheLocation a = new CacheLocation(1, 2);
        assertTrue(a.equals(a)); // same object
        assertFalse(a.equals(null)); // null
        assertFalse(a.equals(new Object())); // different type
        assertTrue(a.equals(new CacheLocation(1, 2))); // same values
    }

    @Test
    void doReload_handlesSupplierException_path() {
        Path tmp = null;
        try {
            tmp = Files.createTempDirectory("cache_doreload_ex");
            CacheManager.Builder b = CacheManager.builder(tmp);
            b.shardCapacity(10).memoryCacheSize(10);
            CacheManager.initialize(b);

            CacheDefinition<String> def = CacheDefinition.<String>builder()
                    .name("failreload")
                    .supplier(() -> { throw new RuntimeException("boom"); })
                    .keyExtractor(CacheRow::getKey)
                    .serializer(CacheDefinition.defaultSerializer())
                    .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                    .ttl(Duration.ofSeconds(60))
                    .build();

            assertThrows(RuntimeException.class, () -> CacheManager.register(def));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (tmp != null) try { Files.walk(tmp).sorted((a,b)->b.compareTo(a)).forEach(p->p.toFile().delete()); } catch (Exception ignored) {}
        }
    }

    @Test
    void checkTtlExpiry_skipsWhenVersionNull() throws Exception {
        Path tmp = Files.createTempDirectory("cache_ttl_vnull");
        CacheManager.Builder b = CacheManager.builder(tmp);
        b.shardCapacity(10).memoryCacheSize(10);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("vnull")
                .supplier(() -> java.util.stream.Stream.of(new CacheRow("k", "v")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofSeconds(60))
                .build();
        CacheManager.register(def);

        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);
        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentHashMap<String, Object> slots = (java.util.concurrent.ConcurrentHashMap<String, Object>) slotsF.get(mgr);
        Object slot = slots.get("vnull");

        Field activeF = slot.getClass().getDeclaredField("activeVersion");
        activeF.setAccessible(true);
        activeF.set(slot, null);

        Method method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(mgr));
    }

    @Test
    void cleanupOldVersion_withNullOld_returnsImmediately() throws Exception {
        Path tmp = Files.createTempDirectory("cache_cleanup_nullold");
        CacheManager.initialize(tmp);
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Method cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, CacheVersion.class, Path.class);
        cleanup.setAccessible(true);

        // should return without exception
        assertDoesNotThrow(() -> cleanup.invoke(mgr, "cname", null, tmp));
    }

    @Test
    void cleanupOldVersion_whenNoReaders_cleansImmediately() throws Exception {
        Path tmp = Files.createTempDirectory("cache_cleanup_noreaders");
        Path versionDir = tmp.resolve("vold2");
        Files.createDirectories(versionDir);
        Files.write(versionDir.resolve("f2"), "x".getBytes());

        CacheVersion<String> v = new CacheVersion<>(versionDir, new CacheShard[0], Map.of(), 0);

        CacheManager.initialize(tmp);
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Method cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, CacheVersion.class, Path.class);
        cleanup.setAccessible(true);
        cleanup.invoke(mgr, "cname", v, versionDir);

        // wait briefly for cleanup thread
        long start = System.currentTimeMillis();
        while (Files.exists(versionDir) && System.currentTimeMillis() - start < 2000) Thread.sleep(50);

        assertFalse(Files.exists(versionDir));
    }

}
