package tr.kontas.cache.example;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.SizedSupplier;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Demonstrates using the builder (not initialize(path)) to configure the manager,
 * shows two caches (simple users and JSON-serialized CustomerData), and a reload
 * while a background reader keeps accessing the cache so we can observe old-version cleanup.
 */
public class ExampleAdvancedCustomization {

    public static void main(String[] args) {
        try {
            Path base = Files.createTempDirectory("cache-example-advanced");
            System.out.println("Cache base dir: " + base.toAbsolutePath());

            // Use builder to customize manager behavior (shard sizes, defaults, index shards)
            CacheManager.Builder builder = CacheManager.builder(base)
                    .shardCapacity(1_000)
                    .memoryCacheSize(100) // default L1 size for caches that don't set it
                    .defaultMaxKeyBytes(64)
                    .defaultMaxValueBytes(512)
                    .indexShardCount(4)
                    .chronicleAverageKey("user-0000");

            // Initialize via builder (shows how customizable the manager is)
            CacheManager.initialize(builder);

            // ---------------- Simple users cache ----------------
            SizedSupplier usersSupplier = SizedSupplier.of(() -> Stream.of(
                    new CacheRow("u1", "alice@example.com"),
                    new CacheRow("u2", "bob@example.com")
            ), 2);

            CacheDefinition<String> users = CacheDefinition.<String>builder()
                    .name("users")
                    .supplier(usersSupplier)
                    .keyExtractor(CacheRow::getKey)
                    .serializer(CacheDefinition.defaultSerializer())
                    .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                    .ttl(Duration.ofHours(1))
                    .memoryCacheMaxSize(50)
                    .build();

            CacheManager.register(users);

            System.out.println("users:u1 -> " + CacheManager.get("users", "u1"));

            // ---------------- Customer cache with JSON serializer ----------------
            AtomicInteger version = new AtomicInteger(0);

            SizedSupplier customerSupplier = SizedSupplier.unknown(() -> {
                int v = version.get();
                return Stream.of(
                        new CacheRow("customer1", new CustomerData("customer1", "Alice", "alice@x.com", 30 + v)),
                        new CacheRow("customer2", new CustomerData("customer2", "Bob", "bob@x.com", 40 + v))
                );
            });

            CacheDefinition<CustomerData> customers = CacheDefinition.<CustomerData>builder()
                    .name("customers")
                    .supplier(customerSupplier)
                    .keyExtractor(CacheRow::getKey)
                    .serializer(row -> ((CustomerData) row.getValue()).toJson().getBytes(StandardCharsets.UTF_8))
                    .deserializer(bytes -> CustomerData.fromJson(new String(bytes, StandardCharsets.UTF_8)))
                    .ttl(Duration.ofMinutes(10))
                    .memoryCacheMaxSize(0) // exercise mmap reads
                    .build();

            CacheManager.register(customers);

            System.out.println("customers:customer1 -> " + CacheManager.get("customers", "customer1"));

            // ---------------- Demonstrate reload while background readers run ----------------
            ExecutorService readerPool = Executors.newSingleThreadExecutor();
            Runnable readerTask = () -> {
                try {
                    for (int i = 0; i < 200; i++) {
                        Object v = CacheManager.get("customers", "customer1");
                        // Print only occasionally to avoid flooding
                        if (i % 50 == 0) System.out.println("bg reader saw: " + v);
                        Thread.sleep(10);
                    }
                } catch (InterruptedException ignored) {
                }
            };

            readerPool.submit(readerTask);

            // Give background reader a moment to start
            Thread.sleep(100);

            // Snapshot directories before reload
            System.out.println("versions before reload: ");
            printVersions(base.resolve("customers"));

            // Change source data and trigger reload
            version.addAndGet(5);
            System.out.println("triggering reload...");
            CacheManager.reload("customers");

            // Immediately show versions after reload — old version may still exist until cleanup
            System.out.println("versions immediately after reload: ");
            printVersions(base.resolve("customers"));

            // Wait for background reader to finish so cleanup can occur
            readerPool.shutdown();
            readerPool.awaitTermination(5, TimeUnit.SECONDS);

            // Wait a bit to allow cleanup task to delete old version directories
            Thread.sleep(500);

            System.out.println("versions after reader stopped and cleanup: ");
            printVersions(base.resolve("customers"));

            System.out.println("ExampleAdvancedCustomization finished.");

        } catch (Throwable t) {
            System.err.println("Example failed: " + t);
            t.printStackTrace();
            System.exit(1);
        }
    }

    private static void printVersions(Path cacheDir) {
        try {
            if (!Files.exists(cacheDir)) {
                System.out.println("  <no directory>");
                return;
            }
            List<Path> versions = Files.list(cacheDir).filter(Files::isDirectory).collect(java.util.stream.Collectors.toList());
            for (Path p : versions) System.out.println("  " + p.getFileName());
            if (versions.isEmpty()) System.out.println("  <none>");
        } catch (Exception e) {
            System.out.println("  (failed to list versions: " + e + ")");
        }
    }

    // Simple CustomerData with JSON (minimal) serialization for the example
    public static final class CustomerData {
        public final String id;
        public final String name;
        public final String email;
        public final int age;

        public CustomerData(String id, String name, String email, int age) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.age = age;
        }

        public String toJson() {
            // Minimal escaping for " only (example only, not production-grade)
            return String.format("{\"id\":\"%s\",\"name\":\"%s\",\"email\":\"%s\",\"age\":%d}",
                    escape(id), escape(name), escape(email), age);
        }

        public static CustomerData fromJson(String json) {
            // Extremely simple parser tailored for the toJson output above.
            try {
                String s = json.trim();
                s = s.substring(1, s.length() - 1); // remove { }
                String[] parts = s.split(",");
                String id = unquote(parts[0].split(":", 2)[1]);
                String name = unquote(parts[1].split(":", 2)[1]);
                String email = unquote(parts[2].split(":", 2)[1]);
                int age = Integer.parseInt(parts[3].split(":", 2)[1]);
                return new CustomerData(id, name, email, age);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse json: " + json, e);
            }
        }

        private static String escape(String s) {
            return s.replace("\"", "\\\"");
        }

        private static String unquote(String q) {
            q = q.trim();
            if (q.startsWith("\"") && q.endsWith("\"")) q = q.substring(1, q.length() - 1);
            return q.replace("\\\"", "\"");
        }

        @Override
        public String toString() {
            return "CustomerData{" + id + "," + name + "," + email + "," + age + '}';
        }
    }
}
