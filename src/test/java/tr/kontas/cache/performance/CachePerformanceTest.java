package tr.kontas.cache.performance;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.SizedSupplier;

import java.lang.management.*;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import java.util.stream.Stream;

// to enable ChronicleMap add all --add-opens and --add-exports VM options
/*
-Xms8g
-Xmx16g
-XX:+UseG1GC
--add-opens
java.base/java.lang=ALL-UNNAMED
--add-opens
java.base/java.lang.reflect=ALL-UNNAMED
--add-opens
java.base/java.nio=ALL-UNNAMED
--add-opens
java.base/sun.nio.ch=ALL-UNNAMED
--add-opens
java.base/java.util=ALL-UNNAMED
--add-opens
java.base/java.io=ALL-UNNAMED
--add-exports
jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
*/
public class CachePerformanceTest {

    private static final int[] DEFAULT_SIZES = {
            1_000,
            10_000,
            100_000,
            1_000_000,
            5_000_000,
            10_000_000,
            50_000_000,
            100_000_000
    };

    // JMX beans for GC and OS monitoring
    private static final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    private static final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    static {
        // Enable thread CPU time measurement if supported
        if (threadBean.isThreadCpuTimeSupported()) {
            threadBean.setThreadCpuTimeEnabled(true);
        }
    }

    public static void main(String[] args) throws Exception {
        printSystemInfo();

        System.out.println("=============== CACHE PERFORMANCE TEST ===============");

        for (int size : DEFAULT_SIZES) {
            runTestForSize(size);
            System.out.println("------------------------------------------------------");
        }

        shutdownCacheManagerExecutors();
        System.out.println("All performance runs completed.");
    }

    private static void runTestForSize(int numRecords) throws Exception {
        System.out.printf("Test başlıyor: %,d kayıt...%n", numRecords);

        // Memory baseline
        System.gc();
        Thread.sleep(300);
        long memBefore = getUsedMemory();

        // GC and I/O baselines
        long gcCountBefore = getGcCount();
        long gcTimeBefore = getGcTime();
        long diskReadBefore = getProcessReadBytes(); // Linux only, returns 0 if unsupported

        Path tempDir = Files.createTempDirectory("cache_perf_" + numRecords);

        CacheManager.Builder builder = CacheManager.builder(tempDir)
                .shardCapacity(100_000)
                .memoryCacheSize(0)
                .defaultMaxKeyBytes(32)
                .defaultMaxValueBytes(256)
                .indexShardCount(16)
                .chronicleAverageKey("key-00000000");

        CacheManager.initialize(builder);

        String cacheName = "perfCache";

        CacheDefinition<String> definition = CacheDefinition.<String>builder()
                .name(cacheName)
                .supplier(SizedSupplier.of(() -> generateData(numRecords), numRecords))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofHours(1))
                .dynamicSizing(true)
                .build();

        // ================= WRITE PHASE =================
        long startWrite = System.currentTimeMillis();
        CacheManager.register(definition);
        long endWrite = System.currentTimeMillis();

        long writeTimeMs = endWrite - startWrite;
        double writeThroughput = (numRecords * 1000.0) / writeTimeMs;
        System.out.printf("Yazma ve İndeksleme: %,d ms (%,.0f kayıt/s)%n", writeTimeMs, writeThroughput);

        // GC after write
        long gcCountWrite = getGcCount() - gcCountBefore;
        long gcTimeWrite = getGcTime() - gcTimeBefore;
        System.out.printf("GC during write: %,d collections, %,d ms%n", gcCountWrite, gcTimeWrite);

        long diskBytes = getFolderSize(tempDir);
        System.out.printf("Disk: %,d MB (%.2f byte/kayıt)%n",
                diskBytes / (1024 * 1024),
                (double) diskBytes / numRecords);

        System.gc();
        Thread.sleep(300);
        long memAfter = getUsedMemory();
        long memUsed = Math.max(0, memAfter - memBefore);
        System.out.printf("Heap: %,d MB (%.2f byte/kayıt)%n",
                memUsed / (1024 * 1024),
                (double) memUsed / numRecords);

        int readCount = Math.min(numRecords, 100_000);
        Random random = new Random(42);

        // Warmup for JIT
        int warmupCount = Math.min(1_000, readCount);
        for (int i = 0; i < warmupCount; i++) {
            CacheManager.get(cacheName, "key-" + random.nextInt(numRecords));
        }

        // ================= READ PHASE =================
        // Reset GC and I/O counters for the read phase
        gcCountBefore = getGcCount();
        gcTimeBefore = getGcTime();
        long diskReadBeforeRead = getProcessReadBytes();
        double cpuLoadBefore = getProcessCpuLoad();

        long[] latenciesNanos = new long[readCount];
        long totalWallNanos = 0;
        long totalCpuNanos = 0;

        for (int i = 0; i < readCount; i++) {
            String key = "key-" + random.nextInt(numRecords);

            long startWall = System.nanoTime();
            long startCpu = threadBean.getCurrentThreadCpuTime(); // returns -1 if not supported
            CacheManager.get(cacheName, key);
            long endCpu = threadBean.getCurrentThreadCpuTime();
            long endWall = System.nanoTime();

            long wallNanos = endWall - startWall;
            latenciesNanos[i] = wallNanos;
            totalWallNanos += wallNanos;

            if (startCpu != -1 && endCpu != -1) {
                totalCpuNanos += (endCpu - startCpu);
            }
        }

        long gcCountRead = getGcCount() - gcCountBefore;
        long gcTimeRead = getGcTime() - gcTimeBefore;
        long diskReadBytes = getProcessReadBytes() - diskReadBeforeRead;
        double cpuLoadAfter = getProcessCpuLoad();

        // Throughput and average latency
        double cacheAvgUs = (totalWallNanos / 1000.0) / readCount;
        double cacheThroughput = (readCount * 1_000_000_000.0) / totalWallNanos;
        System.out.printf("CACHE READ avg:      %.2f µs (%,.0f okuma/s)%n", cacheAvgUs, cacheThroughput);

        // Latency percentiles
        long[] sorted = latenciesNanos.clone();
        Arrays.sort(sorted);
        long p50 = percentile(sorted, 50);
        long p95 = percentile(sorted, 95);
        long p99 = percentile(sorted, 99);
        long p999 = percentile(sorted, 99.9);
        long max = sorted[sorted.length - 1];
        System.out.printf("Latency percentiles: p50=%.2f µs, p95=%.2f µs, p99=%.2f µs, p99.9=%.2f µs, max=%.2f µs%n",
                p50 / 1000.0, p95 / 1000.0, p99 / 1000.0, p999 / 1000.0, max / 1000.0);

        // CPU time per read
        if (totalCpuNanos > 0) {
            double avgCpuUs = (totalCpuNanos / 1000.0) / readCount;
            System.out.printf("Avg CPU time per read: %.2f µs%n", avgCpuUs);
        }

        // GC during read
        System.out.printf("GC during read:  %,d collections, %,d ms%n", gcCountRead, gcTimeRead);

        // Disk I/O during read
        if (diskReadBytes > 0) {
            System.out.printf("Disk read bytes during read: %,d bytes (%.2f MB)%n",
                    diskReadBytes, diskReadBytes / (1024.0 * 1024));
        }

        // Process CPU load
        System.out.printf("Process CPU load: before=%.2f%%, after=%.2f%%%n",
                cpuLoadBefore * 100, cpuLoadAfter * 100);

        // ================= BASELINES =================
        // Baseline 1: Simulated memory access (zero allocation)
        long totalList = 0;
        for (int i = 0; i < readCount; i++) {
            int idx = random.nextInt(numRecords);
            long s = System.nanoTime();
            String key = "key-" + idx;
            String value = "value-" + idx;
            long e = System.nanoTime();
            totalList += (e - s);
            if (key.isEmpty() || value.isEmpty()) throw new AssertionError();
        }
        double listAvgUs = (totalList / 1000.0) / readCount;
        System.out.printf("MEMORY (simulated) avg: %.2f µs%n", listAvgUs);

        // Baseline 2: Object creation cost
        long totalGen = 0;
        for (int i = 0; i < readCount; i++) {
            int idx = random.nextInt(numRecords);
            long s = System.nanoTime();
            new CacheRow("perfCache", "key-" + idx, "value-" + idx);
            long e = System.nanoTime();
            totalGen += (e - s);
        }
        double genAvgUs = (totalGen / 1000.0) / readCount;
        System.out.printf("OBJECT CREATE avg:   %.2f µs%n", genAvgUs);

        // Speedups
        System.out.printf("Speedup vs MEMORY:   x%.2f%n", listAvgUs / cacheAvgUs);
        System.out.printf("Speedup vs OBJECT:   x%.2f%n", genAvgUs / cacheAvgUs);

        // Cleanup
        deleteDirRecursive(tempDir);
    }

    /** Lazy stream — hiçbir kayıt önceden belleğe alınmaz */
    private static Stream<CacheRow> generateData(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new CacheRow("perfCache", "key-" + i, "value-" + i));
    }

    private static long getUsedMemory() {
        Runtime r = Runtime.getRuntime();
        return r.totalMemory() - r.freeMemory();
    }

    private static long getFolderSize(Path dir) {
        try (Stream<Path> s = Files.walk(dir)) {
            return s.filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        try { return Files.size(p); } catch (Exception e) { return 0; }
                    }).sum();
        } catch (Exception e) {
            return 0;
        }
    }

    private static void deleteDirRecursive(Path dir) throws Exception {
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                    });
        }
    }

    private static void shutdownCacheManagerExecutors() {
        try {
            Field f = CacheManager.class.getDeclaredField("TTL_SCHEDULER");
            f.setAccessible(true);
            ((ScheduledExecutorService) f.get(null)).shutdownNow();
        } catch (Exception ignored) {}

        try {
            Field f = CacheManager.class.getDeclaredField("RELOAD_EXECUTOR");
            f.setAccessible(true);
            ((ExecutorService) f.get(null)).shutdownNow();
        } catch (Exception ignored) {}
    }

    // ================= HELPER METHODS FOR METRICS =================

    private static long getGcCount() {
        return gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
    }

    private static long getGcTime() {
        return gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
    }

    private static long getProcessReadBytes() {
        // Linux only: /proc/self/io
        try {
            Path ioStat = Path.of("/proc/self/io");
            if (Files.exists(ioStat)) {
                List<String> lines = Files.readAllLines(ioStat);
                for (String line : lines) {
                    if (line.startsWith("read_bytes:")) {
                        return Long.parseLong(line.split("\\s+")[1]);
                    }
                }
            }
        } catch (Exception ignored) {}
        return 0;
    }

    private static double getProcessCpuLoad() {
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            return ((com.sun.management.OperatingSystemMXBean) osBean).getProcessCpuLoad();
        }
        return Double.NaN;
    }

    private static long percentile(long[] sorted, double percentile) {
        if (sorted.length == 0) return 0;
        double index = (percentile / 100.0) * (sorted.length - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) return sorted[lower];
        double fraction = index - lower;
        return (long) (sorted[lower] * (1 - fraction) + sorted[upper] * fraction);
    }

    public static void printSystemInfo() {
        System.out.println("=============== SYSTEM INFO ===============");

        Runtime runtime = Runtime.getRuntime();

        // CPU
        int availableProcessors = runtime.availableProcessors();
        System.out.println("CPU cores: " + availableProcessors);

        // JVM Memory
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();

        System.out.printf("JVM Max Memory: %.2f GB%n", maxMemory / (1024.0 * 1024 * 1024));
        System.out.printf("JVM Total Memory: %.2f GB%n", totalMemory / (1024.0 * 1024 * 1024));
        System.out.printf("JVM Free Memory: %.2f GB%n", freeMemory / (1024.0 * 1024 * 1024));

        // OS info (Java 11 compatible)
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

        System.out.println("OS Name: " + System.getProperty("os.name"));
        System.out.println("OS Version: " + System.getProperty("os.version"));
        System.out.println("Architecture: " + System.getProperty("os.arch"));

        // Process CPU load (Java 11 -> com.sun.management cast)
        try {
            com.sun.management.OperatingSystemMXBean sunOsBean =
                    (com.sun.management.OperatingSystemMXBean) osBean;

            double processCpuLoad = sunOsBean.getProcessCpuLoad();
            double systemCpuLoad = sunOsBean.getSystemCpuLoad();

            System.out.printf("Process CPU Load: %.2f%%%n", processCpuLoad * 100);
            System.out.printf("System CPU Load: %.2f%%%n", systemCpuLoad * 100);

            // Physical memory (Java 11 supported methods)
            long totalPhysicalMemory = sunOsBean.getTotalPhysicalMemorySize();
            long freePhysicalMemory = sunOsBean.getFreePhysicalMemorySize();

            System.out.printf("Physical Memory: %.2f GB%n", totalPhysicalMemory / (1024.0 * 1024 * 1024));
            System.out.printf("Free Physical Memory: %.2f GB%n", freePhysicalMemory / (1024.0 * 1024 * 1024));

        } catch (Exception e) {
            System.out.println("Extended OS metrics not supported on this JVM.");
        }

        // JVM details
        System.out.println("JVM Name: " + System.getProperty("java.vm.name"));
        System.out.println("JVM Vendor: " + System.getProperty("java.vm.vendor"));
        System.out.println("JVM Version: " + System.getProperty("java.vm.version"));
        System.out.println("Java Version: " + System.getProperty("java.version"));

        System.out.println("===========================================");
    }
}