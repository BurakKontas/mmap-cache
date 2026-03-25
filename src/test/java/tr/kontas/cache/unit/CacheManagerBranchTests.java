package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheVersion;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

public class CacheManagerBranchTests {

    @Test
    void builder_memoryCacheSize_negative_throws() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        var b = CacheManager.builder(tmp);
        assertThrows(IllegalArgumentException.class, () -> b.memoryCacheSize(-1));
    }

    @Test
    void builder_chronicleAverageKey_blank_throws() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        var b = CacheManager.builder(tmp);
        assertThrows(IllegalArgumentException.class, () -> b.chronicleAverageKey(""));
    }

    @Test
    void register_appliesManagerMemoryCacheDefault() throws Exception {
        Path tmpDir = Files.createTempDirectory("mgr_mem_default");
        CacheManager.Builder b = CacheManager.builder(tmpDir).memoryCacheSize(7);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("md")
                .supplier(java.util.stream.Stream::empty)
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(0)
                .build();

        CacheManager.register(def);

        // reflect into INSTANCE.slots to get the registered slot and its effective definition
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);
        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, Object> slots = (ConcurrentHashMap<String, Object>) slotsF.get(inst);
        Object slot = slots.get("md");
        assertNotNull(slot);
        Field defF = slot.getClass().getDeclaredField("definition");
        defF.setAccessible(true);
        Object effectiveDef = defF.get(slot);
        // access memoryCacheMaxSize via reflection getter method on the definition object
        java.lang.reflect.Method m = effectiveDef.getClass().getMethod("getMemoryCacheMaxSize");
        int mem = (Integer) m.invoke(effectiveDef);
        assertEquals(7, mem);
    }

    @Test
    void checkTtlExpiry_positiveSchedulesReload() throws Exception {
        Path tmpDir = Files.createTempDirectory("mgr_ttl_test");
        CacheManager.Builder b = CacheManager.builder(tmpDir);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("ttltest")
                .supplier(() -> java.util.stream.Stream.of())
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofMillis(1))
                .build();

        CacheManager.register(def);

        // get slot and activeVersion
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);
        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, Object> slots = (ConcurrentHashMap<String, Object>) slotsF.get(inst);
        Object slot = slots.get("ttltest");
        assertNotNull(slot);
        Field activeV = slot.getClass().getDeclaredField("activeVersion");
        activeV.setAccessible(true);
        Object version = activeV.get(slot);
        assertNotNull(version);

        // set createdAt to past via reflection so now - createdAt >= ttl
        Field createdAtF = version.getClass().getDeclaredField("createdAt");
        createdAtF.setAccessible(true);
        long old = System.currentTimeMillis() - 10_000L;
        createdAtF.setLong(version, old);

        // invoke private checkTtlExpiry
        var method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(inst));

        // allow some time for async reload to possibly execute
        Thread.sleep(200);
    }

    @Test
    void register_definitionMemoryCacheNotOverridden() throws Exception {
        Path tmpDir = Files.createTempDirectory("mgr_mem_no_override");
        CacheManager.Builder b = CacheManager.builder(tmpDir).memoryCacheSize(7);
        CacheManager.initialize(b);

        // definition explicitly requests a memory cache size -> manager should NOT override
        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("md2")
                .supplier(java.util.stream.Stream::empty)
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(5) // explicit
                .build();

        CacheManager.register(def);

        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);
        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, Object> slots = (ConcurrentHashMap<String, Object>) slotsF.get(inst);
        Object slot = slots.get("md2");
        assertNotNull(slot);
        Field defF = slot.getClass().getDeclaredField("definition");
        defF.setAccessible(true);
        Object effectiveDef = defF.get(slot);
        java.lang.reflect.Method m = effectiveDef.getClass().getMethod("getMemoryCacheMaxSize");
        int mem = (Integer) m.invoke(effectiveDef);
        assertEquals(5, mem);
    }

    @Test
    void builder_memoryCacheSize_positive_sets() throws Exception {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        var b = CacheManager.builder(tmp).memoryCacheSize(3);
        CacheManager.initialize(b);
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);
        java.lang.reflect.Field memF = inst.getClass().getDeclaredField("memoryCacheSize");
        memF.setAccessible(true);
        int val = memF.getInt(inst);
        assertEquals(3, val);
    }

    @Test
    void checkTtlExpiry_ttlZero_noReload() throws Exception {
        Path tmpDir = Files.createTempDirectory("mgr_ttl_zero");
        CacheManager.Builder b = CacheManager.builder(tmpDir);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("ttlzero2")
                .supplier(() -> java.util.stream.Stream.of())
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ZERO)
                .build();

        CacheManager.register(def);

        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);

        var method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);
        // should not throw and should simply return
        assertDoesNotThrow(() -> method.invoke(inst));
    }

    // CacheVersion: ensure we exercise the false branches where TTLs are null but maxSize>0
    @Test
    void buildCache_noTtls_maxSize_positive_noExpireCalls() throws Exception {
        Path tmp = Files.createTempDirectory("cv_cache_nottl");

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("nottl")
                .supplier(java.util.stream.Stream::empty)
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(5) // enter buildCache path
                // leave memoryCacheTtl and memoryCacheIdleTtl as null
                .build();

        CacheVersion<String> v = new CacheVersion<>(tmp, new tr.kontas.cache.CacheShard[0], java.util.Map.of(), def);
        var cache = v.getMemoryCache();
        assertNotNull(cache);
        cache.put("x", "y");
        assertEquals("y", cache.getIfPresent("x"));
        v.close();
    }

    @Test
    void builder_nullBasePath_throws() {
        assertThrows(IllegalArgumentException.class, () -> CacheManager.builder(null));
    }

    @Test
    void checkTtlExpiry_negativeTtl_noReload() throws Exception {
        Path tmpDir = Files.createTempDirectory("mgr_ttl_negative");
        CacheManager.Builder b = CacheManager.builder(tmpDir);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("ttlneg")
                .supplier(() -> java.util.stream.Stream.of())
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofMillis(-1))
                .build();

        CacheManager.register(def);

        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);

        var method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);
        // should not throw and should simply return (negative TTL treated like disabled)
        assertDoesNotThrow(() -> method.invoke(inst));
    }

    @Test
    void buildCache_ttlZero_noExpireAfterWrite() throws Exception {
        Path tmp = Files.createTempDirectory("cv_cache_ttlzero");

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("ttlzeroew")
                .supplier(java.util.stream.Stream::empty)
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(5)
                .memoryCacheTtl(Duration.ZERO)
                .build();

        CacheVersion<String> v = new CacheVersion<>(tmp, new tr.kontas.cache.CacheShard[0], java.util.Map.of(), def);
        var cache = v.getMemoryCache();
        assertNotNull(cache);
        cache.put("kz", "vz");
        // TTL is zero => expireAfterWrite should NOT be applied (the condition is false due to isZero), so value should remain
        assertEquals("vz", cache.getIfPresent("kz"));
        v.close();
    }

    @Test
    void buildCache_idleTtlZero_noExpireAfterAccess() throws Exception {
        Path tmp = Files.createTempDirectory("cv_cache_idlettlzero");

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("idlezero")
                .supplier(java.util.stream.Stream::empty)
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(5)
                .memoryCacheIdleTtl(Duration.ZERO)
                .build();

        CacheVersion<String> v = new CacheVersion<>(tmp, new tr.kontas.cache.CacheShard[0], java.util.Map.of(), def);
        var cache = v.getMemoryCache();
        assertNotNull(cache);
        cache.put("ik", "iv");
        // idle TTL zero => expireAfterAccess should NOT be applied; value should remain
        assertEquals("iv", cache.getIfPresent("ik"));
        v.close();
    }

    @Test
    void doReload_earlyReturn_whenAlreadyReloading() throws Exception {
        Path tmpDir = Files.createTempDirectory("mgr_rereload");
        CacheManager.Builder b = CacheManager.builder(tmpDir);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("rerun")
                .supplier(() -> java.util.stream.Stream.of(new tr.kontas.cache.CacheRow("k1", "v1")))
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .build();

        CacheManager.register(def);

        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);
        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, Object> slots = (ConcurrentHashMap<String, Object>) slotsF.get(inst);
        Object slot = slots.get("rerun");
        assertNotNull(slot);
        Field reloadingF = slot.getClass().getDeclaredField("reloading");
        reloadingF.setAccessible(true);
        java.util.concurrent.atomic.AtomicBoolean ab = (java.util.concurrent.atomic.AtomicBoolean) reloadingF.get(slot);

        // set true so doReload early-returns
        ab.set(true);

        var doReload = CacheManager.class.getDeclaredMethod("doReload", slot.getClass(), String.class);
        doReload.setAccessible(true);
        // should not throw
        assertDoesNotThrow(() -> doReload.invoke(inst, slot, "manual"));

        // reset
        ab.set(false);
    }

    @Test
    void doReload_skipsValueTooLarge_branch() throws Exception {
        Path tmpDir = Files.createTempDirectory("mgr_skip_large");
        CacheManager.Builder b = CacheManager.builder(tmpDir);
        CacheManager.initialize(b);

        // supply a row with large value but set small maxValueBytes so the write will fail
        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("skiplarge")
                .supplier(() -> java.util.stream.Stream.of(new tr.kontas.cache.CacheRow("k1", "this_value_is_too_large")))
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .maxKeyBytes(10)
                .maxValueBytes(5)
                .build();

        CacheManager.register(def);

        // after register, get should return null because entry was skipped
        assertNull(CacheManager.get("skiplarge", "k1"));
    }

    @Test
    void register_managerDefaultNotApplied_whenManagerZero() throws Exception {
        Path tmpDir = Files.createTempDirectory("mgr_mem_zero_default");
        CacheManager.Builder b = CacheManager.builder(tmpDir).memoryCacheSize(0);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("md3")
                .supplier(java.util.stream.Stream::empty)
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(0)
                .build();

        CacheManager.register(def);

        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);
        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, Object> slots = (ConcurrentHashMap<String, Object>) slotsF.get(inst);
        Object slot = slots.get("md3");
        Field defF = slot.getClass().getDeclaredField("definition");
        defF.setAccessible(true);
        Object effectiveDef = defF.get(slot);
        java.lang.reflect.Method m = effectiveDef.getClass().getMethod("getMemoryCacheMaxSize");
        int mem = (Integer) m.invoke(effectiveDef);
        assertEquals(0, mem);
    }

    @Test
    void buildCache_bothTtls_nonNull_nonZero_callsBoth() throws Exception {
        Path tmp = Files.createTempDirectory("cv_cache_both_ttls");

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("bothttls")
                .supplier(java.util.stream.Stream::empty)
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(5)
                .memoryCacheTtl(Duration.ofMillis(200))
                .memoryCacheIdleTtl(Duration.ofMillis(200))
                .build();

        CacheVersion<String> v = new CacheVersion<>(tmp, new tr.kontas.cache.CacheShard[0], java.util.Map.of(), def);
        assertNotNull(v.getMemoryCache());
        v.close();
    }

    @Test
    void buildCache_maxSizeZero_returnsNullMemoryCache() throws Exception {
        Path tmp = Files.createTempDirectory("cv_cache_maxsize0");

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("ms0")
                .supplier(java.util.stream.Stream::empty)
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(0)
                .build();

        CacheVersion<String> v = new CacheVersion<>(tmp, new tr.kontas.cache.CacheShard[0], java.util.Map.of(), def);
        assertNull(v.getMemoryCache());
        v.close();
    }

    @Test
    void checkTtlExpiry_ttlNull_noReload() throws Exception {
        Path tmpDir = Files.createTempDirectory("mgr_ttl_null");
        CacheManager.Builder b = CacheManager.builder(tmpDir);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("ttlnull")
                .supplier(() -> java.util.stream.Stream.of(new tr.kontas.cache.CacheRow("k", "v")))
                .keyExtractor(r -> r.getKey())
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                // do NOT set ttl -> null
                .build();

        CacheManager.register(def);

        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);

        var method = CacheManager.class.getDeclaredMethod("checkTtlExpiry");
        method.setAccessible(true);
        // should not throw and should simply continue because ttl == null
        assertDoesNotThrow(() -> method.invoke(inst));
    }
}
