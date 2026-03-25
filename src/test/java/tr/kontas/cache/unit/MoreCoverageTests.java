package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class MoreCoverageTests {

    @Test
    void initialize_file_null_throws() {
        assertThrows(IllegalArgumentException.class, () -> CacheManager.initialize((File) null));
    }

    @Test
    void initialize_builder_null_throws() {
        assertThrows(IllegalArgumentException.class, () -> CacheManager.initialize((CacheManager.Builder) null));
    }

    @Test
    void get_unknown_after_initialize_returns_null() throws Exception {
        Path tmp = Files.createTempDirectory("mgr_init_test");
        CacheManager.initialize(tmp);
        assertNull(CacheManager.get("no-such-cache", "k"));
    }

    @Test
    void reload_unknown_throws() throws Exception {
        Path tmp = Files.createTempDirectory("mgr_reload_test");
        CacheManager.initialize(tmp);
        assertThrows(IllegalArgumentException.class, () -> CacheManager.reload("unknown-cache"));
    }

    @Test
    void cleanupOldVersion_null_returns_quickly() throws Exception {
        // initialize manager so we have an instance to call the private method on
        Path tmp = Files.createTempDirectory("mgr_cleanup_null");
        CacheManager.initialize(tmp);
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Method cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, Class.forName("tr.kontas.cache.CacheVersion"));
        cleanup.setAccessible(true);
        // invoke on the manager instance, passing null for the old version
        assertDoesNotThrow(() -> cleanup.invoke(mgr, "cname", null));
    }

    @Test
    void doReload_early_return_when_reloading_true() throws Exception {
        Path tmp = Files.createTempDirectory("mgr_doreload_test");
        CacheManager.initialize(tmp);

        // create and register a simple definition
        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("c1")
                .supplier(() -> Stream.of(new CacheRow("k1","v1")))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofHours(1))
                .build();

        CacheManager.register(def);

        // get the manager instance and the slot, set reloading=true via reflection
        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        Field slotsF = CacheManager.class.getDeclaredField("slots");
        slotsF.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> slots = (java.util.Map<String, Object>) slotsF.get(mgr);
        Object slot = slots.get("c1");

        assertNotNull(slot, "Cache slot for 'c1' must be present after register");

        Field reloadingF = slot.getClass().getDeclaredField("reloading");
        reloadingF.setAccessible(true);
        java.util.concurrent.atomic.AtomicBoolean reloading = (java.util.concurrent.atomic.AtomicBoolean) reloadingF.get(slot);

        assertNotNull(reloading, "Slot.reloading must be present and non-null");

        // set true to simulate concurrent reload
        reloading.set(true);

        // call reload which will internally call doReload — should return early and not throw
        assertDoesNotThrow(() -> CacheManager.reload("c1"));

        // cleanup: reset flag
        reloading.set(false);
    }
}
