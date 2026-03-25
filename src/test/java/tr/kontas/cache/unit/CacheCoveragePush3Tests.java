package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.CacheShard;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doThrow;

public class CacheCoveragePush3Tests {

    @Test
    void cacheShard_close_handlesIOExceptionFromChannel() throws Exception {
        Path tmp = Files.createTempDirectory("cache_close_io");
        int maxKey = 4, maxVal = 4;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        CacheShard shard = new CacheShard(tmp.resolve("s_close.dat"), recordSize, 2, maxKey, maxVal);

        // mock channel to throw on close
        FileChannel mockChannel = Mockito.mock(FileChannel.class);
        doThrow(new IOException("mock close fail")).when(mockChannel).close();

        Field channelF = CacheShard.class.getDeclaredField("channel");
        channelF.setAccessible(true);
        channelF.set(shard, mockChannel);

        Field bufF = CacheShard.class.getDeclaredField("buffer");
        bufF.setAccessible(true);
        // Do not mock MappedByteBuffer on Java 21; leave buffer null to exercise close path safely
        bufF.set(shard, null);

        // should swallow the IOException and not throw
        assertDoesNotThrow(shard::close);
    }

    @Test
    void cacheDefinition_builder_toString_allFields() {
        var b = CacheDefinition.<String>builder();
        b.name("xx");
        b.supplier(() -> java.util.stream.Stream.empty());
        b.keyExtractor(CacheRow::getKey);
        b.serializer(CacheDefinition.defaultSerializer());
        b.deserializer(CacheDefinition.defaultDeserializer(s -> s));
        b.ttl(java.time.Duration.ofSeconds(1));
        b.dynamicSizing(true);
        b.maxKeyBytes(16);
        b.maxValueBytes(32);

        // call toString on the builder instance directly
        String s = b.toString();
        assertDoesNotThrow(() -> {
            if (s == null) throw new RuntimeException("toString returned null");
        });
    }
}
