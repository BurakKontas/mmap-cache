package tr.kontas.cache.example;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.SizedSupplier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Spawns many concurrent reader threads that continuously read from the cache while
 * a reloader thread performs reloads periodically. Shows that reads continue to work
 * and old versions are cleaned up after readers finish.
 */
public class ExampleConcurrentReaders {
    public static void main(String[] args) throws Exception {
        Path base = Files.createTempDirectory("cache-example-concurrent");
        System.out.println("Cache base dir: " + base.toAbsolutePath());

        CacheManager.Builder builder = CacheManager.builder(base)
                .shardCapacity(1000)
                .memoryCacheSize(0)
                .indexShardCount(4);
        CacheManager.initialize(builder);

        int N = 5000;
        SizedSupplier supplier = SizedSupplier.of(() -> IntStream.range(0, N)
                .mapToObj(i -> new CacheRow("k-" + i, "v-" + i)), N);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("concurrent")
                .supplier(supplier)
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ZERO)
                .dynamicSizing(true)
                .build();

        CacheManager.register(def);

        AtomicInteger version = new AtomicInteger(0);

        ExecutorService readers = Executors.newFixedThreadPool(16);
        Runnable reader = () -> {
            Random rnd = new Random();
            for (int i = 0; i < 1000; i++) {
                String k = "k-" + rnd.nextInt(N);
                Object v = CacheManager.get("concurrent", k);
                if (i % 250 == 0) System.out.println("reader saw: " + (v == null ? "<null>" : "ok"));
                try { Thread.sleep(2); } catch (InterruptedException ignored) {}
            }
        };

        for (int i = 0; i < 16; i++) readers.submit(reader);

        // Reloader thread
        ExecutorService reloader = Executors.newSingleThreadExecutor();
        reloader.submit(() -> {
            try {
                for (int r = 0; r < 5; r++) {
                    Thread.sleep(150);
                    System.out.println("manual reload #" + (r + 1));
                    CacheManager.reload("concurrent");
                    version.incrementAndGet();
                }
            } catch (InterruptedException ignored) {}
        });

        readers.shutdown();
        readers.awaitTermination(10, TimeUnit.SECONDS);

        reloader.shutdown();
        reloader.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("ExampleConcurrentReaders finished.");
    }
}
