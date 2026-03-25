package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheShard;
import tr.kontas.cache.CacheVersion;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CacheLockedFilesTests {

    @Test
    void purgeStaleVersions_handlesLockedFileDeletes() throws Exception {
        Path tmp = Files.createTempDirectory("cache_locked_purge");
        Path cacheDir = tmp.resolve("c1");
        Path vdir = cacheDir.resolve("v1");
        Files.createDirectories(vdir);
        Path lockedFile = vdir.resolve("file.dat");
        Files.write(lockedFile, "data".getBytes());

        // keep an open stream to lock the file on Windows so delete may fail
        try (OutputStream os = new FileOutputStream(lockedFile.toFile())) {
            // Invoke purgeStaleVersions reflectively; should not throw even if delete fails
            Method m = CacheManager.class.getDeclaredMethod("purgeStaleVersions", java.nio.file.Path.class);
            m.setAccessible(true);
            assertDoesNotThrow(() -> m.invoke(null, tmp));
        }

        // cleanup
        Files.walk(tmp).sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
    }

    @Test
    void cleanupOldVersion_handlesLockedFileDeletes() throws Exception {
        Path tmp = Files.createTempDirectory("cache_locked_cleanup");
        Path versionDir = tmp.resolve("vdir");
        Files.createDirectories(versionDir);
        Path locked = versionDir.resolve("f.dat");
        Files.write(locked, "x".getBytes());

        CacheShard[] shards = new CacheShard[0];
        CacheVersion<String> v = new CacheVersion<>(versionDir, shards, Map.of(), 0);

        // initialize manager so we can call cleanupOldVersion
        CacheManager.initialize(tmp);
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Method cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, CacheVersion.class, Path.class);
        cleanup.setAccessible(true);

        // hold file open to cause delete to fail on Windows
        try (OutputStream os = new FileOutputStream(locked.toFile())) {
            assertDoesNotThrow(() -> cleanup.invoke(mgr, "cname", v, versionDir));
        }

        // give cleanup thread time to run
        Thread.sleep(200);

        // cleanup
        Files.walk(tmp).sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
    }
}
