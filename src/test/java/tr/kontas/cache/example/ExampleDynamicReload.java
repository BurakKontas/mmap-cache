package tr.kontas.cache.example;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.SizedSupplier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Standalone example (main) demonstrating a dynamic supplier and manual reloads.
 * This is not a mock test: it runs the actual cache code and writes data to disk.
 */
public class ExampleDynamicReload {

    public static void main(String[] args) {
        try {
            Path temp = Files.createTempDirectory("cache-example-dyn");
            System.out.println("Cache base dir: " + temp.toAbsolutePath());

            CacheManager.initialize(temp);

            // Simulate a changing data source using an atomic counter and a supplier
            AtomicInteger counter = new AtomicInteger(0);

            SizedSupplier supplier = SizedSupplier.unknown(() -> {
                int base = counter.get();
                return Stream.of(
                        new CacheRow("item-1", "value-" + base),
                        new CacheRow("item-2", "value-" + (base + 1))
                );
            });

            CacheDefinition<String> def = CacheDefinition.<String>builder()
                    .name("dynamic")
                    .supplier(supplier)
                    .keyExtractor(CacheRow::getKey)
                    .serializer(CacheDefinition.defaultSerializer())
                    .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                    .ttl(Duration.ofMillis(100)) // small TTL so background reload can trigger
                    .memoryCacheMaxSize(0) // disable L1 to exercise mmap reads
                    .dynamicSizing(true)
                    .build();

            CacheManager.register(def);

            // Initial values
            System.out.println("initial item-1 -> " + CacheManager.get("dynamic", "item-1"));

            // Simulate data change and trigger manual reload
            counter.addAndGet(10);
            System.out.println("triggering manual reload...");
            CacheManager.reload("dynamic");

            // Wait briefly for reload to complete
            TimeUnit.MILLISECONDS.sleep(200);

            System.out.println("after reload item-1 -> " + CacheManager.get("dynamic", "item-1"));

            // Also verify async reload
            counter.addAndGet(5);
            System.out.println("triggering async reload...");
            CacheManager.reloadAsync("dynamic");
            TimeUnit.MILLISECONDS.sleep(200);
            System.out.println("after async reload item-1 -> " + CacheManager.get("dynamic", "item-1"));

            System.out.println("ExampleDynamicReload finished.");
        } catch (Throwable t) {
            System.err.println("Example failed: " + t);
            t.printStackTrace();
            System.exit(1);
        }
    }
}
