package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheShard;
import tr.kontas.cache.CacheVersion;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mockStatic;

public class CacheFilesStaticMockTests {

    @Test
    void purgeStaleVersions_listThrows_isHandled() throws Exception {
        Path base = Paths.get("/nonexistent-base");
        try (MockedStatic<Files> mocked = mockStatic(Files.class)) {
            mocked.when(() -> Files.exists(base)).thenReturn(true);
            mocked.when(() -> Files.list(base)).thenThrow(new RuntimeException("list-fail"));

            Method m = CacheManager.class.getDeclaredMethod("purgeStaleVersions", Path.class);
            m.setAccessible(true);
            assertDoesNotThrow(() -> m.invoke(null, base));
        }
    }

    @Test
    void purgeStaleVersions_innerListThrows_isHandled() throws Exception {
        Path base = Paths.get("/base");
        Path cacheDir = base.resolve("c1");
        try (MockedStatic<Files> mocked = mockStatic(Files.class)) {
            mocked.when(() -> Files.exists(base)).thenReturn(true);
            mocked.when(() -> Files.list(base)).thenReturn(Stream.of(cacheDir));
            mocked.when(() -> Files.isDirectory(cacheDir)).thenReturn(true);
            mocked.when(() -> Files.list(cacheDir)).thenThrow(new RuntimeException("inner-list-fail"));

            Method m = CacheManager.class.getDeclaredMethod("purgeStaleVersions", Path.class);
            m.setAccessible(true);
            assertDoesNotThrow(() -> m.invoke(null, base));
        }
    }

    @Test
    void purgeStaleVersions_walkThrows_isHandled() throws Exception {
        Path base = Paths.get("/base2");
        Path cacheDir = base.resolve("c2");
        Path versionDir = cacheDir.resolve("v123");

        try (MockedStatic<Files> mocked = mockStatic(Files.class)) {
            mocked.when(() -> Files.exists(base)).thenReturn(true);
            mocked.when(() -> Files.list(base)).thenReturn(Stream.of(cacheDir));
            mocked.when(() -> Files.isDirectory(cacheDir)).thenReturn(true);
            mocked.when(() -> Files.list(cacheDir)).thenReturn(Stream.of(versionDir));
            mocked.when(() -> Files.isDirectory(versionDir)).thenReturn(true);
            mocked.when(() -> Files.walk(versionDir)).thenThrow(new RuntimeException("walk-fail"));

            Method m = CacheManager.class.getDeclaredMethod("purgeStaleVersions", Path.class);
            m.setAccessible(true);
            assertDoesNotThrow(() -> m.invoke(null, base));
        }
    }

    @Test
    void cleanupOldVersion_walkThrows_isHandled_inThread() throws Exception {
        Path versionDir = Paths.get("/somev");
        Path base = Paths.get("/base3");
        CacheShard[] shards = new CacheShard[0];
        CacheVersion<String> v = new CacheVersion<>(versionDir, shards, java.util.Map.of(), 0);

        // initialize manager so cleanupOldVersion can be invoked on instance
        CacheManager.initialize(base);
        var instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Method cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, CacheVersion.class, Path.class);
        cleanup.setAccessible(true);

        try (MockedStatic<Files> mocked = mockStatic(Files.class)) {
            // make walk throw when thread runs
            mocked.when(() -> Files.walk(versionDir)).thenThrow(new RuntimeException("walk-thread-fail"));
            // Also ensure deleteIfExists throws to hit inner catch if reached
            mocked.when(() -> Files.deleteIfExists(versionDir)).thenThrow(new RuntimeException("delete-fail"));

            // invoke cleanup (starts a thread). Keep mock active while thread runs.
            assertDoesNotThrow(() -> cleanup.invoke(mgr, "cname", v, versionDir));

            // wait briefly to allow background thread to run while mock is active
            Thread.sleep(300);
        }
    }

    @Test
    void purgeStaleVersions_nonexistentPath_returnsSilently() throws Exception {
        Path base = Paths.get("nonexistent_path_for_purge_12345");
        // ensure it doesn't exist
        if (Files.exists(base)) Files.walk(base).sorted((a,b)->b.compareTo(a)).forEach(p->p.toFile().delete());

        var m = CacheManager.class.getDeclaredMethod("purgeStaleVersions", Path.class);
        m.setAccessible(true);
        assertDoesNotThrow(() -> m.invoke(null, base));
    }
}
