package tr.kontas.cache.example;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Real-world-like example: product catalog caching using Jackson JSON serializer.
 * Shows builder usage, custom serializer/deserializer for complex objects, and reload behavior.
 */
public class ExampleProductCatalog {

    private static final ObjectMapper MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    public static void main(String[] args) throws Exception {
        Path base = Files.createTempDirectory("cache-example-products");
        System.out.println("Cache base dir: " + base.toAbsolutePath());

        CacheManager.Builder builder = CacheManager.builder(base)
                .memoryCacheSize(200)
                .defaultMaxValueBytes(2048)
                .shardCapacity(1000)
                .indexShardCount(4);

        CacheManager.initialize(builder);

        // Simulated "database" that we can mutate and then reload the cache
        AtomicInteger rev = new AtomicInteger(0);

        SizedSupplier supplier = SizedSupplier.unknown(() -> {
            int r = rev.get();
            Product p1 = new Product("p1", "Laptop X", "electronics", 1299.99 + r);
            p1.attributes.put("brand", "BrandCo");
            p1.attributes.put("warranty", r % 2 == 0 ? "2 years" : "3 years");

            Product p2 = new Product("p2", "Coffee Mug", "kitchen", 9.99 + r);
            p2.attributes.put("material", "ceramic");

            return Stream.of(
                    new CacheRow(p1.getId(), p1),
                    new CacheRow(p2.getId(), p2)
            );
        });

        CacheDefinition<Product> products = CacheDefinition.<Product>builder()
                .name("products")
                .supplier(supplier)
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
                .ttl(Duration.ofMinutes(5))
                .memoryCacheMaxSize(100)
                .dynamicSizing(true)
                .build();

        CacheManager.register(products);

        // Initial read
        System.out.println("Initial read p1 -> " + CacheManager.get("products", "p1"));

        // Simulate DB change and manual reload
        System.out.println("Simulating DB change and reloading...");
        rev.addAndGet(10);
        CacheManager.reload("products");
        TimeUnit.MILLISECONDS.sleep(200);

        System.out.println("After reload p1 -> " + CacheManager.get("products", "p1"));

        // Show that L1 cache can keep values (read twice)
        Product first = CacheManager.get("products", "p1");
        Product second = CacheManager.get("products", "p1");
        System.out.println("Two reads equal: " + (first != null && first.getPrice() == second.getPrice()));

        System.out.println("ExampleProductCatalog finished.");
    }

    @Getter
    public static final class Product {
        private String id;
        private String name;
        private String category;
        private double price;
        private final Map<String, String> attributes = new HashMap<>();

        // Default constructor for Jackson
        public Product() {}

        public Product(String id, String name, String category, double price) {
            this.id = id;
            this.name = name;
            this.category = category;
            this.price = price;
        }

        @Override
        public String toString() {
            return "Product{" + id + "," + name + "," + category + "," + price + ",attrs=" + attributes + '}';
        }
    }
}
