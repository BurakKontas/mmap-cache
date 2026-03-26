package tr.kontas.cache.performance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.Getter;
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.SizedSupplier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Benchmark that loads 1 million products into the cache and then
 * measures concurrent read throughput and latency.
 */
public class ProductCatalogBenchmark {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private static final int PRODUCT_COUNT = 1_000_000;
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final Duration TEST_DURATION = Duration.ofSeconds(10);

    public static void main(String[] args) throws Exception {
        CachePerformanceTest.printSystemInfo();

        // 1. Set up a temporary directory for the cache
        Path base = Files.createTempDirectory("cache-benchmark-products");
        System.out.println("Cache base dir: " + base.toAbsolutePath());

        // 2. Configure the cache manager for large datasets
        CacheManager.Builder builder = CacheManager.builder(base)
                .memoryCacheSize(10_000)                // keep 10k hottest entries in memory
                .defaultMaxValueBytes(4_096)            // each product is < 4KB
                .shardCapacity(200_000)                 // each shard can hold 200k entries
                .indexShardCount(16);                   // 16 shards → total capacity 3.2M

        CacheManager.initialize(builder);

        // 3. Define the product cache
        CacheDefinition<Product> products = CacheDefinition.<Product>builder()
                .name("products")
                .supplier(createProductSupplier())       // supplier that generates 1M products
                .keyExtractor(CacheRow::getKey)
                .serializer(row -> {
                    try {
                        return MAPPER.writeValueAsBytes(row.getValue());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .deserializer(bytes -> {
                    try {
                        return MAPPER.readValue(bytes, Product.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .ttl(Duration.ofHours(1))                // long TTL for benchmarking
                .memoryCacheMaxSize(0)              // same as manager's memory size
                .dynamicSizing(true)                     // adapt to changing sizes
                .build();

        CacheManager.register(products);

        // 4. Load all 1M products into the cache (reload)
        System.out.println("Populating cache with " + PRODUCT_COUNT + " products...");
        long startLoad = System.nanoTime();
        CacheManager.reload("products");
        long loadTime = System.nanoTime() - startLoad;
        System.out.printf("Loaded %d products in %.2f seconds%n",
                PRODUCT_COUNT, loadTime / 1e9);

        // 5. Warm‑up – let the cache stabilise
        System.out.println("Warming up...");
        Random warmupRandom = new Random();
        for (int i = 0; i < 10_000; i++) {
            String key = "product_" + warmupRandom.nextInt(PRODUCT_COUNT);
            CacheManager.get("products", key);
        }

        // 6. Benchmark: concurrent reads with throughput & latency measurement
        System.out.printf("Starting benchmark with %d threads for %d seconds...%n",
                THREAD_COUNT, TEST_DURATION.getSeconds());

        AtomicLong operations = new AtomicLong(0);
        AtomicLong totalLatencyNanos = new AtomicLong(0);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREAD_COUNT; t++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();                     // all threads start together
                    Random random = new Random();
                    long endTime = System.nanoTime() + TEST_DURATION.toNanos();

                    while (System.nanoTime() < endTime) {
                        String key = "product_" + random.nextInt(PRODUCT_COUNT);
                        long start = System.nanoTime();
                        Product p = CacheManager.get("products", key);
                        if (p == null) {
                            throw new AssertionError("Product not found: " + key);
                        }
                        long latency = System.nanoTime() - start;
                        operations.incrementAndGet();
                        totalLatencyNanos.addAndGet(latency);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        // Start the clock
        startLatch.countDown();

        // Wait for all threads to finish
        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();

        // 7. Print results
        long ops = operations.get();
        long totalLatency = totalLatencyNanos.get();
        double avgLatencyMs = totalLatency / (double) ops / 1_000_000.0;
        double throughput = ops / TEST_DURATION.getSeconds();

        System.out.printf("Completed %d operations in %d seconds%n", ops, TEST_DURATION.getSeconds());
        System.out.printf("Throughput: %.2f ops/sec%n", throughput);
        System.out.printf("Average latency: %.3f ms%n", avgLatencyMs);
        System.out.println("Benchmark finished.");
    }

    /**
     * Creates a supplier that lazily streams 1 million products.
     * The stream is not materialised in memory all at once.
     */
    private static SizedSupplier createProductSupplier() {
        return SizedSupplier.unknown(() -> {
            return Stream.iterate(0, i -> i + 1)
                    .limit(PRODUCT_COUNT)
                    .map(i -> {
                        Product p = new Product(
                                "product_" + i,
                                "Product " + i,
                                "cat_" + (i % 100),
                                i * 0.01
                        );
                        p.getAttributes().put("attr1", "value" + i);
                        return new CacheRow(p.getId(), p);
                    });
        });
    }

    /**
     * Simple product class with Jackson‑compatible constructors/getters.
     */
    @Getter
    public static final class Product {
        private String id;
        private String name;
        private String category;
        private double price;
        private final Map<String, String> attributes = new HashMap<>();

        public Product() {} // required for Jackson

        public Product(String id, String name, String category, double price) {
            this.id = id;
            this.name = name;
            this.category = category;
            this.price = price;
        }
    }
}