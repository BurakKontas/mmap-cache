package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import tr.kontas.cache.CacheLocation;
import tr.kontas.cache.CacheShard;
import tr.kontas.cache.CacheVersion;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

public class CacheVersionCloseExceptionTest {

    @Test
    void cacheVersion_close_handlesExceptionFromCloseableIndex() throws Exception {
        Path tmp = Files.createTempDirectory("chr_close_exception_test");

        @SuppressWarnings("unchecked")
        Map<String, CacheLocation> index = Mockito.mock(Map.class, withSettings().extraInterfaces(Closeable.class));

        Closeable closeable = (Closeable) index;
        Mockito.doThrow(new RuntimeException("simulated close failure")).when(closeable).close();

        CacheVersion<String> version = new CacheVersion<>(tmp, new CacheShard[0], index, TestHelpers.simpleDefinition("cvclose", 0));

        // Should not throw even if index.close() throws — this exercises the catch block inside CacheVersion.close()
        assertDoesNotThrow(version::close);

        // Verify close was called on the Closeable index
        verify(closeable).close();
    }
}
