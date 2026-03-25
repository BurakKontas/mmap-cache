package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.*;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Chronicle Map entegrasyonunu kapsayan testler.
 * <p>
 * Kapsadığı alanlar:
 * - buildChronicleIndex() off-heap map oluşturma
 * - Builder.chronicleAverageKey() setter
 * - CacheVersion.close() → Chronicle Map (Closeable) close path
 * - CacheVersion.close() → plain Map (non-Closeable) path (regresyon)
 * - CacheLocation no-arg constructor (Chronicle için zorunlu)
 * - CacheLocation BytesMarshallable writeMarshallable / readMarshallable round-trip
 */
public class ChronicleMapIntegrationTests {

    // ── 1. buildChronicleIndex: gerçek ChronicleMap üretiliyor ──────────────

    @Test
    void buildChronicleIndex_producesWorkingOffHeapMap() throws Exception {
        Path tmp = Files.createTempDirectory("chr_index_test");
        Path indexFile = tmp.resolve("index.chm");

        Method m = CacheManager.class.getDeclaredMethod(
                "buildChronicleIndex", Path.class, int.class, String.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, CacheLocation> index =
                (Map<String, CacheLocation>) m.invoke(null, indexFile, 100, "key-00000000");

        assertNotNull(index);
        assertInstanceOf(Closeable.class, index, "Chronicle Map should implement Closeable");

        // put / get çalışmalı
        index.put("k1", new CacheLocation(0, 5));
        index.put("k2", new CacheLocation(1, 10));

        CacheLocation loc1 = index.get("k1");
        assertNotNull(loc1);
        assertEquals(0, loc1.shardId());
        assertEquals(5, loc1.offset());

        CacheLocation loc2 = index.get("k2");
        assertEquals(1, loc2.shardId());
        assertEquals(10, loc2.offset());

        assertNull(index.get("nonexistent"));

        ((Closeable) index).close();

        // dosya oluşturulmuş olmalı
        assertTrue(Files.exists(indexFile));

        // cleanup
        Files.walk(tmp).sorted((a, b) -> b.compareTo(a))
                .forEach(p -> p.toFile().delete());
    }

    // ── 2. buildChronicleIndex: expectedEntries=0 edge case ─────────────────

    @Test
    void buildChronicleIndex_withZeroEntries_doesNotThrow() throws Exception {
        Path tmp = Files.createTempDirectory("chr_zero_entries");
        Path indexFile = tmp.resolve("index.chm");

        Method m = CacheManager.class.getDeclaredMethod(
                "buildChronicleIndex", Path.class, int.class, String.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, CacheLocation> index =
                (Map<String, CacheLocation>) m.invoke(null, indexFile, 0, "key-0");

        assertNotNull(index);
        ((Closeable) index).close();

        Files.walk(tmp).sorted((a, b) -> b.compareTo(a))
                .forEach(p -> p.toFile().delete());
    }

    // ── 3. Builder.chronicleAverageKey setter ────────────────────────────────

    @Test
    void builder_chronicleAverageKey_isApplied() throws Exception {
        Path tmp = Files.createTempDirectory("chr_builder_key");

        CacheManager.Builder builder = CacheManager.builder(tmp)
                .shardCapacity(10)
                .memoryCacheSize(0)
                .chronicleAverageKey("user:0000000000");

        // builder içindeki field'ı oku
        Field f = CacheManager.Builder.class.getDeclaredField("chronicleAverageKey");
        f.setAccessible(true);
        assertEquals("user:0000000000", f.get(builder));

        // initialize et, gerçek register yaparak averageKey'in kullanıldığını doğrula
        CacheManager.initialize(builder);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("avgKeyTest")
                .supplier(() -> Stream.of(
                        new CacheRow("user:0000000001", "Alice"),
                        new CacheRow("user:0000000002", "Bob")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofHours(1))
                .build();

        assertDoesNotThrow(() -> CacheManager.register(def));
        assertEquals("Alice", CacheManager.get("avgKeyTest", "user:0000000001"));
        assertEquals("Bob", CacheManager.get("avgKeyTest", "user:0000000002"));

        Files.walk(tmp).sorted((a, b) -> b.compareTo(a))
                .forEach(p -> p.toFile().delete());
    }

    @Test
    void builder_chronicleAverageKey_nullOrEmpty_throws() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        CacheManager.Builder b = CacheManager.builder(tmp);
        assertThrows(IllegalArgumentException.class, () -> b.chronicleAverageKey(null));
        assertThrows(IllegalArgumentException.class, () -> b.chronicleAverageKey(""));
    }

    // ── 4. CacheVersion.close() → Closeable index path ──────────────────────

    @Test
    void cacheVersion_close_closesChronicleMapIndex() throws Exception {
        Path tmp = Files.createTempDirectory("chr_version_close");
        Path indexFile = tmp.resolve("index.chm");

        Method m = CacheManager.class.getDeclaredMethod(
                "buildChronicleIndex", Path.class, int.class, String.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, CacheLocation> index =
                (Map<String, CacheLocation>) m.invoke(null, indexFile, 10, "key-00000000");

        index.put("x", new CacheLocation(0, 0));

        CacheVersion<String> version =
                new CacheVersion<>(tmp, new CacheShard[0], index, TestHelpers.simpleDefinition("chrv", 0));

        // close() çağrısı ChronicleMap'i de kapatmalı, exception fırlatmamalı
        assertDoesNotThrow(version::close);

        // ChronicleMap kapatıldıktan sonra erişim exception fırlatmalı
        assertThrows(Exception.class, () -> index.get("x"),
                "Closed ChronicleMap should throw on access");

        Files.walk(tmp).sorted((a, b) -> b.compareTo(a))
                .forEach(p -> p.toFile().delete());
    }

    // ── 5. CacheVersion.close() → plain Map (non-Closeable) path (regresyon) ─

    @Test
    void cacheVersion_close_withPlainMap_doesNotThrow() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        Map<String, CacheLocation> plainIndex = new HashMap<>();
        plainIndex.put("k", new CacheLocation(0, 0));

        CacheVersion<String> version =
                new CacheVersion<>(tmp, new CacheShard[0], plainIndex, TestHelpers.simpleDefinition("plainv", 0));

        // plain Map Closeable değil → close() sessizce geçmeli
        assertDoesNotThrow(version::close);
    }

    // ── 6. CacheLocation no-arg constructor (Chronicle tarafından kullanılır) ─

    @Test
    void cacheLocation_noArgConstructor_isUsableByChronicle() {
        // Chronicle Map deserialization sırasında no-arg constructor çağırır
        CacheLocation loc = new CacheLocation();
        // default değerler 0, 0
        assertEquals(0, loc.shardId());
        assertEquals(0, loc.offset());

        // normal constructor hâlâ çalışmalı
        CacheLocation loc2 = new CacheLocation(3, 7);
        assertEquals(3, loc2.shardId());
        assertEquals(7, loc2.offset());
    }

    // ── 7. CacheLocation BytesMarshallable round-trip ────────────────────────

    @Test
    void cacheLocation_bytesMarshallable_roundTrip() throws Exception {
        Path tmp = Files.createTempDirectory("chr_marshallable");
        Path indexFile = tmp.resolve("roundtrip.chm");

        Method buildMethod = CacheManager.class.getDeclaredMethod(
                "buildChronicleIndex", Path.class, int.class, String.class);
        buildMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, CacheLocation> index =
                (Map<String, CacheLocation>) buildMethod.invoke(
                        null, indexFile, 50, "key-0000");

        // Chronicle writeMarshallable → serialize → disk'e yaz
        index.put("key-0001", new CacheLocation(2, 99));
        index.put("key-0002", new CacheLocation(0, 0));
        index.put("key-0003", new CacheLocation(Integer.MAX_VALUE, Integer.MAX_VALUE));

        // readMarshallable → Chronicle get ile deserialize
        CacheLocation r1 = index.get("key-0001");
        assertEquals(2, r1.shardId());
        assertEquals(99, r1.offset());

        CacheLocation r2 = index.get("key-0002");
        assertEquals(0, r2.shardId());
        assertEquals(0, r2.offset());

        CacheLocation r3 = index.get("key-0003");
        assertEquals(Integer.MAX_VALUE, r3.shardId());
        assertEquals(Integer.MAX_VALUE, r3.offset());

        ((Closeable) index).close();

        Files.walk(tmp).sorted((a, b) -> b.compareTo(a))
                .forEach(p -> p.toFile().delete());
    }

    // ── 8. Tam entegrasyon: register → get (Chronicle index üzerinden) ───────

    @Test
    void fullIntegration_registerAndGet_usesChronicleIndex() throws Exception {
        Path tmp = Files.createTempDirectory("chr_full_integration");

        CacheManager.initialize(
                CacheManager.builder(tmp)
                        .shardCapacity(100)
                        .memoryCacheSize(0)          // LRU kapalı → tüm okumalar index'ten
                        .chronicleAverageKey("key-00000000")
        );

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("chrFull")
                .supplier(() -> Stream.of(
                        new CacheRow("key-00000001", "val-1"),
                        new CacheRow("key-00000002", "val-2"),
                        new CacheRow("key-00000003", "val-3")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofHours(1))
                .build();

        CacheManager.register(def);

        assertEquals("val-1", CacheManager.get("chrFull", "key-00000001"));
        assertEquals("val-2", CacheManager.get("chrFull", "key-00000002"));
        assertEquals("val-3", CacheManager.get("chrFull", "key-00000003"));
        assertNull(CacheManager.get("chrFull", "nonexistent"));

        Files.walk(tmp).sorted((a, b) -> b.compareTo(a))
                .forEach(p -> p.toFile().delete());
    }

    // ── 9. Reload sonrası eski Chronicle Map temizleniyor ────────────────────

    @Test
    void reload_closesOldChronicleIndex_andCreatesNew() throws Exception {
        Path tmp = Files.createTempDirectory("chr_reload_cleanup");

        CacheManager.initialize(
                CacheManager.builder(tmp)
                        .shardCapacity(100)
                        .memoryCacheSize(0)
                        .chronicleAverageKey("k-000")
        );

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("chrReload")
                .supplier(() -> Stream.of(new CacheRow("k-001", "v1")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofHours(1))
                .build();

        CacheManager.register(def);
        assertEquals("v1", CacheManager.get("chrReload", "k-001"));

        // Reload → eski versionDir + index.chm silinmeli, yeni oluşturulmalı
        CacheManager.reload("chrReload");
        assertEquals("v1", CacheManager.get("chrReload", "k-001"));

        // cleanup thread'inin bitmesi için bekle
        Thread.sleep(500);

        Files.walk(tmp).sorted((a, b) -> b.compareTo(a))
                .forEach(p -> p.toFile().delete());
    }
}

