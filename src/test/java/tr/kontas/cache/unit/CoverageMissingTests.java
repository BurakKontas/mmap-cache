package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheLocation;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheShard;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CoverageMissingTests {

    @Test
    void cacheLocation_noArg_and_equals_hashcode_toString() throws Exception {
        // no-arg constructor
        CacheLocation loc = new CacheLocation();

        // set private fields via reflection to cover no-arg usage paths
        Field sF = CacheLocation.class.getDeclaredField("shardId");
        Field oF = CacheLocation.class.getDeclaredField("offset");
        sF.setAccessible(true);
        oF.setAccessible(true);
        sF.setInt(loc, 5);
        oF.setInt(loc, 42);

        assertEquals(5, loc.shardId());
        assertEquals(42, loc.offset());

        // param constructor and equals/hashCode/toString
        CacheLocation a = new CacheLocation(5, 42);
        CacheLocation b = new CacheLocation(5, 42);
        CacheLocation c = new CacheLocation(1, 2);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a CacheLocation"));
        assertTrue(a.toString().contains("shardId=5"));
    }

    @Test
    void cacheManager_buildChronicleIndex_and_purgeStaleVersions() throws Exception {
        Path tmp = Files.createTempDirectory("cov_mgr_base");
        // create a sample cache/version dir structure for purgeStaleVersions
        Path cacheDir = tmp.resolve("mycache");
        Path versionDir = cacheDir.resolve("v123");
        Files.createDirectories(versionDir);
        Files.createFile(versionDir.resolve("dummy.txt"));

        // call purgeStaleVersions via reflection - should remove the version dir
        Method purge = CacheManager.class.getDeclaredMethod("purgeStaleVersions", Path.class);
        purge.setAccessible(true);
        purge.invoke(null, tmp);

        assertFalse(Files.exists(versionDir));

        // buildChronicleIndex: ensure returns a Closeable map and the file exists
        Path indexFile = Files.createTempDirectory("cov_mgr_idx").resolve("index.chm");
        // NOTE: production code changed to add an extra boolean param (useChronicleMap).
        Method build = CacheManager.class.getDeclaredMethod("buildChronicleIndex", Path.class, int.class, String.class, int.class, boolean.class);
        build.setAccessible(true);
        @SuppressWarnings("unchecked")
        int avgBytes = "key-0000".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        Map<String, Object> idx = (Map<String, Object>) build.invoke(null, indexFile, 1, "key-0000", avgBytes, true);

        assertNotNull(idx);
        assertTrue(idx instanceof Closeable || idx.getClass().getName().contains("Fallback"));
        assertTrue(Files.exists(indexFile));

        // close if possible
        if (idx instanceof Closeable) ((Closeable) idx).close();
    }

    @Test
    void cacheShard_outOfBounds_read_write_throw() throws Exception {
        Path tmp = Files.createTempDirectory("cov_shard");
        int maxKey = 4, maxVal = 4;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        CacheShard shard = new CacheShard(tmp.resolve("s.dat"), recordSize, 1, maxKey, maxVal);

        // write beyond capacity (offset == capacity) should throw
        assertThrows(IndexOutOfBoundsException.class, () -> shard.write(1, new tr.kontas.cache.CacheEntry(1, 1L, "k", new byte[]{1}, maxKey, maxVal)));
        // read beyond capacity should throw
        assertThrows(IndexOutOfBoundsException.class, () -> shard.read(1));

        shard.close();
    }

    @Test
    void fallbackCloseableMap_behaviour_via_reflection() throws Exception {
        Class<?> cls = Class.forName("tr.kontas.cache.CacheManager$FallbackCloseableMap");
        var ctor = cls.getDeclaredConstructor();
        ctor.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, String> m = (java.util.Map<String, String>) ctor.newInstance();
        // put/get before close
        m.put("k", "v");
        assertEquals("v", m.get("k"));
        // remove/clear work
        m.remove("k");
        m.put("a", "b");
        m.clear();
        // close and ensure operations throw
        java.lang.reflect.Method close = cls.getDeclaredMethod("close");
        close.setAccessible(true);
        close.invoke(m);
        assertThrows(IllegalStateException.class, () -> m.get("a"));
        assertThrows(IllegalStateException.class, () -> m.put("x", "y"));
        assertThrows(IllegalStateException.class, m::clear);
    }

    @Test
    void cacheLocation_bytesMarshallable_with_proxy_bytes_in_out() throws Exception {
        CacheLocation loc = new CacheLocation(13, 1313);
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(8);

        // Proxy for BytesOut: handle writeInt(int)
        Class<?> bytesOutClazz = net.openhft.chronicle.bytes.BytesOut.class;
        Object bytesOutProxy = java.lang.reflect.Proxy.newProxyInstance(
                bytesOutClazz.getClassLoader(),
                new Class[]{bytesOutClazz},
                (proxy, method, args) -> {
                    if ("writeInt".equals(method.getName()) && args != null && args.length == 1) {
                        bb.putInt((Integer) args[0]);
                        return null;
                    }
                    // other calls: no-op or return defaults
                    if (method.getReturnType().isPrimitive()) return 0;
                    return null;
                }
        );

        // call writeMarshallable
        net.openhft.chronicle.bytes.BytesOut<?> out = (net.openhft.chronicle.bytes.BytesOut<?>) bytesOutProxy;
        loc.writeMarshallable(out);

        // prepare ByteBuffer for reading
        bb.flip();

        // Proxy for BytesIn: handle readInt()
        Class<?> bytesInClazz = net.openhft.chronicle.bytes.BytesIn.class;
        Object bytesInProxy = java.lang.reflect.Proxy.newProxyInstance(
                bytesInClazz.getClassLoader(),
                new Class[]{bytesInClazz},
                (proxy, method, args) -> {
                    if ("readInt".equals(method.getName()) && (args == null || args.length == 0)) {
                        return bb.getInt();
                    }
                    // default values for other methods
                    if (method.getReturnType().isPrimitive()) return 0;
                    return null;
                }
        );

        net.openhft.chronicle.bytes.BytesIn<?> in = (net.openhft.chronicle.bytes.BytesIn<?>) bytesInProxy;
        CacheLocation r = new CacheLocation();
        r.readMarshallable(in);
        assertEquals(loc, r);
    }

}
