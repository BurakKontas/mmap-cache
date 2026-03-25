package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import tr.kontas.cache.CacheShard;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

public class CacheShardConstructorExceptionTest {

    @Test
    void constructor_rethrowsRuntime_whenInitializeThrowsIOException() throws Exception {
        Path p = Paths.get("cache-shard-constructor-exception-test.dat");
        // ensure any real file is absent to avoid interference
        // but we rely on mocking FileChannel.open to throw

        try (MockedStatic<FileChannel> mocked = mockStatic(FileChannel.class)) {
            mocked.when(() -> FileChannel.open(
                    p,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            )).thenThrow(new IOException("mock open fail"));

            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> new CacheShard(p.toString(), 16 + 16 + Long.BYTES, 1, 16, 16));
            assertTrue(ex.getMessage().contains("Failed to create cache shard"));
        }
    }
}
