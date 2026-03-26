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
 * Standalone example (main) demonstrating a simple user lookup cache.
 * Designed to be run directly by users (no JUnit). Prints results and leaves
 * cache files on disk for inspection.
 */
public class ExampleSimpleUsage {

    public static void main(String[] args) {
        try {
            Path temp = Files.createTempDirectory("cache-example-users");
            System.out.println("Cache base dir: " + temp.toAbsolutePath());

            // initialize manager (normally done once at application startup)
            CacheManager.initialize(temp);

            // Supplier that returns two user records (id -> email)
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
                    .ttl(Duration.ZERO) // no automatic TTL reload
                    .memoryCacheMaxSize(10) // small on-heap L1 cache
                    .build();

            CacheManager.register(users);

            System.out.println("users:u1 -> " + CacheManager.get("users", "u1"));
            System.out.println("users:u2 -> " + CacheManager.get("users", "u2"));
            System.out.println("users:missing -> " + CacheManager.get("users", "not-there"));

            System.out.println("ExampleSimpleUsage finished.");
        } catch (Throwable t) {
            System.err.println("Example failed: " + t);
            t.printStackTrace();
            System.exit(1);
        }
    }
}
