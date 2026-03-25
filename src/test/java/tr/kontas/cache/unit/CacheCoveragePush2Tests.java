package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.CacheShard;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class CacheCoveragePush2Tests {

    @Test
    void builder_toString_viaReflection() throws Exception {
        var builder = CacheDefinition.<String>builder();
        builder.name("b");
        builder.supplier(() -> java.util.stream.Stream.empty());
        builder.keyExtractor(CacheRow::getKey);
        builder.serializer(CacheDefinition.defaultSerializer());
        builder.deserializer(CacheDefinition.defaultDeserializer(s -> s));
        builder.ttl(java.time.Duration.ZERO);

        // invoke toString via declared method to ensure generated method is hit
        Method toStringM = builder.getClass().getDeclaredMethod("toString");
        toStringM.setAccessible(true);
        Object res = toStringM.invoke(builder);
        assertNotNull(res);

        // also call toBuilder().toString()
        Object built = builder.build().toBuilder();
        Method tb = built.getClass().getDeclaredMethod("toString");
        tb.setAccessible(true);
        assertNotNull(tb.invoke(built));
    }

    @Test
    void cacheShard_unmap_tryPath() throws Exception {
        Path tmp = Files.createTempDirectory("cache_unmap");
        int maxKey = 4, maxVal = 4;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        CacheShard shard = new CacheShard(tmp.resolve("s_unmap.dat"), recordSize, 2, maxKey, maxVal);

        // get private buffer field
        Field bufF = CacheShard.class.getDeclaredField("buffer");
        bufF.setAccessible(true);
        MappedByteBuffer buf = (MappedByteBuffer) bufF.get(shard);
        assertNotNull(buf);

        // invoke private static unmap method
        Method unmap = CacheShard.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
        unmap.setAccessible(true);
        assertDoesNotThrow(() -> unmap.invoke(null, buf));

        shard.close();
    }

    @Test
    void cacheRow_equals_negativeCases() {
        CacheRow a = new CacheRow("t", "k", "v");
        assertFalse(a.equals(null));
        assertFalse(a.equals(new Object()));
        CacheRow b = new CacheRow("t", "k", "v");
        assertTrue(a.equals(b));
    }
}
