package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheEntry;
import tr.kontas.cache.CacheShard;
import tr.kontas.cache.CacheLocation;
import tr.kontas.cache.CacheManager;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CoverageAdditionsTests {

    @Test
    void cacheEntry_serialize_deserialize_roundtrip() {
        CacheEntry e1 = new CacheEntry(1L, 2L, "k1", "v1".getBytes(), 32, 32);
        ByteBuffer buf = ByteBuffer.allocate(e1.getMaxKeyBytes() + e1.getMaxValueBytes() + 32);
        boolean ok = e1.serialize(buf);
        assertTrue(ok);
        buf.flip();

        CacheEntry e2 = new CacheEntry(32, 32);
        e2.deserialize(buf);
        assertEquals(e1.getKey(), e2.getKey());
        assertArrayEquals(e1.getValueBytes(), e2.getValueBytes());
    }

    @Test
    void cacheShard_outOfBounds_read_write_throw() throws Exception {
        Path tmp = Files.createTempDirectory("cs_out");
        CacheShard shard = new CacheShard(tmp.resolve("s.dat"), 64, 2, 10, 10);

        CacheEntry entry = new CacheEntry(1L, 1L, "k", new byte[1], 10, 10);
        // valid writes
        assertTrue(shard.write(0, entry));
        assertTrue(shard.write(1, entry));

        // out of bounds write should throw
        assertThrows(IndexOutOfBoundsException.class, () -> shard.write(2, entry));
        assertThrows(IndexOutOfBoundsException.class, () -> shard.read(2));

        shard.close();
        // closed shard operations throw
        assertThrows(IllegalStateException.class, () -> shard.write(0, entry));
        assertThrows(IllegalStateException.class, () -> shard.read(0));
    }

    @Test
    void fallbackCloseableMap_behaviour_close_then_access_throws() throws Exception {
        // Build a fallback map via reflective buildChronicleIndex (small invocation)
        Path tmp = Files.createTempDirectory("fcm_test");
        Path idx = tmp.resolve("index.chm");

        Method m = CacheManager.class.getDeclaredMethod("buildChronicleIndex", java.nio.file.Path.class, int.class, String.class, int.class, boolean.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, CacheLocation> map = (Map<String, CacheLocation>) m.invoke(null, idx, 1, "k-0", 3, false);
        assertNotNull(map);
        if (map instanceof Closeable) {
            ((Closeable) map).close();
            assertThrows(Exception.class, () -> map.get("x"));
        } else {
            // fallback map: close is no-op but should not throw
            // try put/get
            map.put("a", new CacheLocation(0, 0));
            assertNotNull(map.get("a"));
        }
    }

    @Test
    void purgeStaleVersions_deletes_version_dir() throws Exception {
        Path tmp = Files.createTempDirectory("purge_test");
        Path cacheDir = tmp.resolve("c1");
        Path vdir = cacheDir.resolve("v100");
        Files.createDirectories(vdir);
        Files.createFile(vdir.resolve("t.txt"));

        Method purge = CacheManager.class.getDeclaredMethod("purgeStaleVersions", java.nio.file.Path.class);
        purge.setAccessible(true);
        purge.invoke(null, tmp);

        assertFalse(Files.exists(vdir));
    }
}
