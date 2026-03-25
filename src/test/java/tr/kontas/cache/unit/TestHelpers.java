package tr.kontas.cache.unit;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheRow;

import java.time.Duration;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public final class TestHelpers {

    private TestHelpers() {
    }

    public static <V> CacheDefinition<V> simpleDefinition(String name, int memoryCacheMaxSize) {
        return CacheDefinition.<V>builder()
                .name(name)
                .supplier(Stream::empty)
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> (V) s))
                .ttl(Duration.ZERO)
                .memoryCacheMaxSize(memoryCacheMaxSize)
                .build();
    }
}
