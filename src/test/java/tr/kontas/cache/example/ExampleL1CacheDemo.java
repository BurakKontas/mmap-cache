package tr.kontas.cache.example;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.SizedSupplier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;

/**
 * Demonstrates the L1 memory cache (Caffeine): first read is slower (mmap), subsequent reads
 * should be faster due to in-memory hits. Also demonstrates expire-after-write TTL behavior.
 */
public class ExampleL1CacheDemo {
    public static void main(String[] args) throws Exception {
        Path base = Files.createTempDirectory("cache-example-l1");
        System.out.println("Cache base dir: " + base.toAbsolutePath());

        CacheManager.Builder builder = CacheManager.builder(base)
                .memoryCacheSize(100)
                .shardCapacity(1000);

        CacheManager.initialize(builder);

        SizedSupplier supplier = SizedSupplier.of(() -> Stream.of(
                new CacheRow("k1", "v1"),
                new CacheRow("k2", "v2")
        ), 2);

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name("l1demo")
                .supplier(supplier)
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .memoryCacheMaxSize(10)
                .memoryCacheTtl(Duration.ofMillis(250)) // expire-after-write quickly
                .build();

        CacheManager.register(def);

        // Warm read (first time) — expected to be slower
        long t1 = System.nanoTime();
        System.out.println("read1: " + CacheManager.get("l1demo", "k1"));
        long d1 = System.nanoTime() - t1;

        // Immediate second read — should be faster due to in-memory hit
        long t2 = System.nanoTime();
        System.out.println("read2: " + CacheManager.get("l1demo", "k1"));
        long d2 = System.nanoTime() - t2;

        System.out.printf("first read: %d µs, second read: %d µs%n", d1 / 1000, d2 / 1000);

        // Wait for memoryCacheTtl to expire then read again — should be slower as it reloads from mmap
        Thread.sleep(400);
        long t3 = System.nanoTime();
        System.out.println("read3 after TTL: " + CacheManager.get("l1demo", "k1"));
        long d3 = System.nanoTime() - t3;

        System.out.printf("read after TTL: %d µs%n", d3 / 1000);

        System.out.println("ExampleL1CacheDemo finished.");
    }
}
