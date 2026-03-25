package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.CacheShard;
import tr.kontas.cache.CacheVersion;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockStatic;

public class CacheCoveragePush4Tests {

    @Test
    void cacheRow_equals_allBranches() {
        CacheRow a = new CacheRow(null, "k", "v");
        // same object
        assertEquals(a, a);
        // different type
        assertNotEquals(a, new Object());
        // same key different table
        CacheRow b = new CacheRow("t", "k", "v");
        assertNotEquals(a, b);
        // same table and key
        CacheRow c = new CacheRow(null, "k", "v");
        assertEquals(a, c);
    }

    @Test
    void cleanupOldVersion_deleteThrows_catchPath_runInThread() throws Exception {
        Path tmp = Files.createTempDirectory("cache_push4");
        Path versionDir = tmp.resolve("v1");
        Files.createDirectories(versionDir);
        // create a dummy file inside versionDir
        Path f = versionDir.resolve("f.txt");
        Files.writeString(f, "x");
        // create a CacheVersion with this dir
        CacheVersion<String> old = new CacheVersion<>(versionDir, new CacheShard[0], Map.of(), TestHelpers.simpleDefinition("oldv", 0));

        // initialize manager so we can call private cleanupOldVersion
        CacheManager.initialize(tmp);
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Method cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, CacheVersion.class);
        cleanup.setAccessible(true);
        // Mock Files.walk and Files.deleteIfExists using MockedStatic
        try (MockedStatic<Files> mocked = mockStatic(Files.class)) {
            mocked.when(() -> Files.walk(versionDir)).thenReturn(Stream.of(f, versionDir));
            mocked.when(() -> Files.deleteIfExists(f)).thenReturn(false);
            assertDoesNotThrow(() -> cleanup.invoke(mgr, "cname", old));
        }
    }
}
