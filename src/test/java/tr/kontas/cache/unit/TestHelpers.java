package tr.kontas.cache.unit;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheRow;

import java.time.Duration;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public final class TestHelpers {
    static {
        // Ensure Byte Buddy experimental features are enabled during tests (Java 25+ compatibility)
        // This helps Mockito's inline mocking to work when running tests from IDE.
        System.setProperty("net.bytebuddy.experimental", "true");
    }

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
