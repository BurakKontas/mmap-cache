package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheShard;
import tr.kontas.cache.CacheVersion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mockStatic;

public class CacheManagerExceptionBranchesTest {

    @Test
    void purgeStaleVersions_whenFilesListThrows() throws Exception {
        Path base = Paths.get("nonexistent-base");
        try (MockedStatic<Files> mocked = mockStatic(Files.class)) {
            // make Files.exists return true so method proceeds
            mocked.when(() -> Files.exists(base)).thenReturn(true);
            // make Files.list(base) throw
            mocked.when(() -> Files.list(base)).thenThrow(new RuntimeException("list-fail"));

            // call method and ensure it doesn't throw
            var m = CacheManager.class.getDeclaredMethod("purgeStaleVersions", Path.class);
            m.setAccessible(true);
            assertDoesNotThrow(() -> m.invoke(null, base));
        }
    }

    @Test
    void cleanupOldVersion_whenFilesWalkThrows() throws Exception {
        // create a dummy CacheVersion with a path that will cause Files.walk to throw
        Path path = Paths.get("some-path");
        CacheShard[] shards = new CacheShard[0];
        CacheVersion<String> v = new CacheVersion<>(path, shards, java.util.Map.of(), TestHelpers.simpleDefinition("exc", 0));
        try (var mocked = mockStatic(Files.class)) {
            // Files.walk will throw when called on v.getVersionDir()
            mocked.when(() -> Files.walk(v.getVersionDir())).thenThrow(new RuntimeException("walk-fail"));
            // create a dummy CacheManager instance via builder initialize
            Path tmp = Files.createTempDirectory("cmex");
            CacheManager.initialize(tmp);
            var instF = CacheManager.class.getDeclaredField("INSTANCE");
            instF.setAccessible(true);
            Object mgr = instF.get(null);
            var cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, CacheVersion.class);
            cleanup.setAccessible(true);
            // should not throw even if Files.walk throws inside
            assertDoesNotThrow(() -> cleanup.invoke(mgr, "exc", v));
        }
    }
}
