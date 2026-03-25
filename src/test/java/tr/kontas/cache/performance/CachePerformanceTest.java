package tr.kontas.cache.performance;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CachePerformanceTest {

    // default sizes; can be overridden by passing comma-separated sizes as program argument
    private static final int[] DEFAULT_SIZES = {
            1_000,
            10_000,
            100_000,
            1_000_000,
            10_000_000,
            //100_000_000
    };

    public static void main(String[] args) throws Exception {
        System.out.println("=============== CACHE PERFORMANCE TEST ===============");

        int[] sizes = DEFAULT_SIZES;
        if (args != null && args.length > 0 && args[0] != null && !args[0].isBlank()) {
            try {
                String[] parts = args[0].split(",");
                List<Integer> parsed = new ArrayList<>();
                for (String p : parts) {
                    parsed.add(Integer.parseInt(p.trim()));
                }
                sizes = parsed.stream().mapToInt(Integer::intValue).toArray();
            } catch (Exception e) {
                System.err.println("Failed to parse sizes from args, falling back to defaults: " + e.getMessage());
                sizes = DEFAULT_SIZES;
            }
        }

        for (int size : sizes) {
            runTestForSize(size);
            System.out.println("------------------------------------------------------");
        }

        // best-effort shutdown of manager executors and threads
        shutdownCacheManagerExecutors();

        System.out.println("All performance runs completed.");
    }

    private static void runTestForSize(int numRecords) throws Exception {
        System.out.printf("Test başlıyor: %,d kayıt...%n", numRecords);

        // mild GC + pause to stabilize
        System.gc();
        Thread.sleep(300);

        long memBefore = getUsedMemory();

        Path tempDir = Files.createTempDirectory("cache_perf_" + numRecords);

        // Cache Manager init
        CacheManager.Builder builder = CacheManager.builder(tempDir)
                .shardCapacity(100_000)
                .memoryCacheSize(0)
                .defaultMaxKeyBytes(32)
                .defaultMaxValueBytes(64)
                .chronicleAverageKey("key-00000000");

        CacheManager.initialize(builder);

        String cacheName = "perfCache";

        CacheDefinition<String> definition = CacheDefinition.<String>builder()
                .name(cacheName)
                .supplier(() -> generateData(numRecords))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofHours(1))
                .dynamicSizing(true)
                .build();

        long startWrite = System.currentTimeMillis();
        CacheManager.register(definition);
        long endWrite = System.currentTimeMillis();

        System.out.printf("Yazma ve İndeksleme (%d kayıt): %,d ms%n", numRecords, (endWrite - startWrite));

        // Disk size (walk the tempDir to sum file sizes)
        long diskBytes = getFolderSize(tempDir);
        System.out.printf("Diskte Kapladığı Alan: %,d MB (%,d Byte)%n",
                diskBytes / (1024 * 1024), diskBytes);

        // Memory usage
        System.gc();
        Thread.sleep(300);

        long memAfter = getUsedMemory();
        long memUsed = memAfter - memBefore;

        System.out.printf("Yaklaşık Heap Kullanımı (Index vs): %,d MB%n",
                Math.max(0, memUsed) / (1024 * 1024));

        // Read test
        int readCount = Math.min(numRecords, 100_000);
        Random random = new Random(42);

        // Warmup
        for (int i = 0; i < 1000 && i < readCount; i++) {
            CacheManager.get(cacheName, "key-" + random.nextInt(numRecords));
        }

        long totalNanos = 0;
        long maxNanos = 0;
        long minNanos = Long.MAX_VALUE;

        for (int i = 0; i < readCount; i++) {
            String qKey = "key-" + random.nextInt(numRecords);

            long start = System.nanoTime();
            CacheManager.get(cacheName, qKey);
            long end = System.nanoTime();

            long elapsed = end - start;

            totalNanos += elapsed;
            if (elapsed > maxNanos) maxNanos = elapsed;
            if (elapsed < minNanos) minNanos = elapsed;
        }

        double avgUs = (totalNanos / 1000.0) / readCount;
        double maxMs = maxNanos / 1_000_000.0;
        double minUs = minNanos / 1000.0;

        System.out.printf("Rastgele Okuma (%d adet):%n", readCount);
        System.out.printf(" - Ortalama: %,.2f µs%n", avgUs);
        System.out.printf(" - Min:      %,.2f µs%n", minUs);
        System.out.printf(" - Max:      %,.2f ms%n", maxMs);

        // delete dir (best-effort)
        try {
            deleteDirRecursive(tempDir);
        } catch (Throwable t) {
            System.err.println("Failed to delete temp dir " + tempDir + ": " + t.getMessage());
        }
    }

    /**
     * ✅ Lazy + streaming data generation (OOM-safe)
     */
    private static Stream<CacheRow> generateData(int count) {
        return IntStream.range(0, count).mapToObj(i ->
                new CacheRow("perfCache", "key-" + i, "value-for-record-" + i)
        );
    }

    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static long getFolderSize(Path dir) {
        try (Stream<Path> stream = Files.walk(dir)) {
            return stream.filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (Exception e) {
                            return 0L;
                        }
                    }).sum();
        } catch (Exception e) {
            return 0L;
        }
    }

    private static void deleteDirRecursive(Path dir) throws Exception {
        if (!Files.exists(dir)) return;
        // delete files and directories in reverse order
        try (Stream<Path> stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (Exception ignored) {
                        }
                    });
        }
    }

    private static void shutdownCacheManagerExecutors() {
        try {
            Field f = CacheManager.class.getDeclaredField("TTL_SCHEDULER");
            f.setAccessible(true);
            Object sched = f.get(null);

            if (sched instanceof ScheduledExecutorService) {
                ScheduledExecutorService ses = (ScheduledExecutorService) sched;
                ses.shutdownNow();
                ses.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (Throwable ignored) {
        }

        try {
            Field f2 = CacheManager.class.getDeclaredField("RELOAD_EXECUTOR");
            f2.setAccessible(true);
            Object exec = f2.get(null);

            if (exec instanceof ExecutorService) {
                ExecutorService es = (ExecutorService) exec;
                es.shutdownNow();
                es.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (Throwable ignored) {
        }

        // interrupt lingering cache-* threads
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t == null) continue;

            String name = t.getName();

            if (name != null && name.startsWith("cache-")) {
                try {
                    if (t.isAlive()) {
                        t.interrupt();
                        t.join(2_000);

                        if (t.isAlive()) {
                            System.err.println("Warning: cache thread still alive: " + name);
                        }
                    }
                } catch (Throwable ignored) {
                }
            }
        }
    }
}