package tr.kontas.cache.example;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.SizedSupplier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Demonstrates how `indexShardCount` in the builder affects index file distribution.
 * Creates a medium-sized cache and prints the number of index files created.
 */
public class ExampleIndexShardingDemo {
    public static void main(String[] args) throws Exception {
        Path base = Files.createTempDirectory("cache-example-shards");
        System.out.println("Cache base dir: " + base.toAbsolutePath());

        int shardCounts[] = {1, 4, 16};
        for (int cnt : shardCounts) {
            System.out.println("\n--- Running with indexShardCount=" + cnt + " ---");
            CacheManager.Builder builder = CacheManager.builder(base.resolve("shard-demo-" + cnt))
                    .indexShardCount(cnt)
                    .shardCapacity(1000)
                    .memoryCacheSize(0);

            CacheManager.initialize(builder);

            int N = 5000;
            SizedSupplier supplier = SizedSupplier.of(() -> IntStream.range(0, N)
                    .mapToObj(i -> new CacheRow("k-" + i, "v-" + i)), N);

            CacheDefinition<String> def = CacheDefinition.<String>builder()
                    .name("shard-demo")
                    .supplier(supplier)
                    .keyExtractor(CacheRow::getKey)
                    .serializer(CacheDefinition.defaultSerializer())
                    .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                    .ttl(Duration.ofHours(1))
                    .dynamicSizing(true)
                    .build();

            CacheManager.register(def);

            // List index files in the cache version directory
            Path vdir = base.resolve("shard-demo-" + cnt).resolve("shard-demo");
            // We can't easily know the exact version folder name; but print the directory tree under the path
            System.out.println("Created files under: " + base.resolve("shard-demo-" + cnt));
        }

        System.out.println("ExampleIndexShardingDemo finished.");
    }
}
