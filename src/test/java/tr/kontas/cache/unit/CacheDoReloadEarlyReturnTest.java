package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CacheDoReloadEarlyReturnTest {

    @Test
    void doReload_earlyReturn_whenAlreadyReloading() throws Exception {
        Path tmp = Files.createTempDirectory("cache_doreload_early");
        CacheManager.Builder b = CacheManager.builder(tmp);
        b.shardCapacity(10).memoryCacheSize(10);
        CacheManager.initialize(b);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("early")
                .supplier(() -> Stream.of(new CacheRow("k", "v")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofSeconds(60))
                .build();
        CacheManager.register(def);

        // get slot via reflection
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object inst = instF.get(null);
        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentHashMap<String, Object> slots = (java.util.concurrent.ConcurrentHashMap<String, Object>) slotsF.get(inst);
        Object slot = slots.get("early");

        // set reloading to true
        Field relF = slot.getClass().getDeclaredField("reloading");
        relF.setAccessible(true);
        java.util.concurrent.atomic.AtomicBoolean ab = new java.util.concurrent.atomic.AtomicBoolean(true);
        relF.set(slot, ab);

        Method doReload = CacheManager.class.getDeclaredMethod("doReload", slot.getClass(), String.class);
        doReload.setAccessible(true);

        // should early return and not throw
        assertDoesNotThrow(() -> doReload.invoke(inst, slot, "manual"));
    }
}
