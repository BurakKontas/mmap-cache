package tr.kontas.cache.performance;

import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheManager;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.SizedSupplier;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/*
 * mvn clean package -DskipTests
 *
 * java --add-opens java.base/java.lang=ALL-UNNAMED \
 *      --add-opens java.base/java.nio=ALL-UNNAMED  \
 *      --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
 *      -Xms4g -Xmx4g -XX:+UseG1GC               \
 *      -cp target/benchmarks.jar                  \
 *      tr.kontas.cache.benchmark.CacheBenchmarkRunner
 *
 * Sonuçlar:
 *   results/incremental-results.csv  — her kombinasyon biter bitmez yazılır
 *   results/metrics.csv              — GC / heap / disk ek metrikleri
 *   results/checkpoints/*.done       — yeniden başlatmada tamamlananlar atlanır
 */
public class CacheBenchmarkRunner {

    // =========================================================================
    // PARAMETRE UZAYI
    // =========================================================================

    static final int[] SIZES = {1_000, 10_000, 100_000, 1_000_000, 10_000_000};
    static final int[] SHARD_CAPS = {100_000, 500_000};
    static final int[] INDEX_SHARDS = {32, 64, 128};
    static final String[] PATTERNS = {"UNIFORM", "HOT_80_20"};
    static final int[] HIT_RATES = {100};
    static final int[] VALUE_SIZES = {256};
    static final int[] MEM_CACHE_SIZES = {0, 10_000};
    static final boolean[] ALLOC_GARBAGE = {false, true};
    static final boolean[] USE_CHRONICLE = {true, false};
    static final int[] THREAD_COUNTS = {1, 4, 8};

    // =========================================================================
    // ZAMANLAMA
    // =========================================================================

    /**
     * Warmup süresi — sonuçlara dahil edilmez
     */
    static final int WARMUP_SECONDS = 5;
    /**
     * Ölçüm süresi — sonuçlara dahil edilir
     */
    static final int MEASURE_SECONDS = 15;
    /**
     * Her thread'in tutabileceği max örnek sayısı (reservoir)
     */
    static final int SAMPLES_PER_THREAD = 200_000;
    /**
     * Reload kaç kez tekrar edilsin
     */
    static final int RELOAD_REPS = 3;

    static final String CACHE_NAME = "benchCache";

    // =========================================================================
    // DİZİNLER
    // =========================================================================

    static final Path RESULTS_DIR = Path.of("results");
    static final Path CHECKPOINT_DIR = RESULTS_DIR.resolve("checkpoints");
    static final Path RESULTS_CSV = RESULTS_DIR.resolve("incremental-results.csv");
    static final Path METRICS_CSV = RESULTS_DIR.resolve("metrics.csv");
    static final Path CACHE_DIR = Path.of("benchmark-cache-dir");

    static final Object WRITE_LOCK = new Object();

    static {
        mkdir(RESULTS_DIR);
        mkdir(CHECKPOINT_DIR);
    }

    // =========================================================================
    // MAIN
    // =========================================================================

    public static void main(String[] args) throws Exception {
        List<Config> all = buildConfigs();
        System.out.printf("Toplam kombinasyon: %d%n%n", all.size());

        int ran = 0, skipped = 0, failed = 0;
        for (int i = 0; i < all.size(); i++) {
            Config cfg = all.get(i);
            String key = cfg.key();

            if (Checkpoint.isDone(key)) {
                System.out.printf("[%d/%d] SKIP  %s%n", i + 1, all.size(), key);
                skipped++;
                continue;
            }

            System.out.printf("%n[%d/%d] START %s%n", i + 1, all.size(), key);
            try {
                runOne(cfg);
                ran++;
                System.out.printf("[%d/%d] OK    %s%n", i + 1, all.size(), key);
            } catch (Exception e) {
                failed++;
                System.err.printf("[%d/%d] FAIL  %s — %s%n", i + 1, all.size(), key, e.getMessage());
                e.printStackTrace(System.err);
            }
        }

        System.out.printf("%nBitti. Çalıştırılan=%d  Atlanan=%d  Hata=%d%n", ran, skipped, failed);
    }

    // =========================================================================
    // KOMBİNASYON ÜRETME
    // =========================================================================

    static List<Config> buildConfigs() {
        List<Config> list = new ArrayList<>();
        for (int size : SIZES)
            for (int shardCap : SHARD_CAPS)
                for (int idxShard : INDEX_SHARDS)
                    for (String pat : PATTERNS)
                        for (int hitRate : HIT_RATES)
                            for (int valSize : VALUE_SIZES)
                                for (int memCache : MEM_CACHE_SIZES)
                                    for (boolean garbage : ALLOC_GARBAGE)
                                        for (boolean chron : USE_CHRONICLE)
                                            for (int threads : THREAD_COUNTS)
                                                list.add(new Config(size, shardCap, idxShard, pat,
                                                        hitRate, valSize, memCache, garbage, chron, threads));
        return list;
    }

    // =========================================================================
    // BİR KOMBİNASYON ÇALIŞTIRMA
    // =========================================================================

    static void runOne(Config cfg) throws Exception {

        CacheManager.closeAll();

        deleteDirectoryRecursively(CACHE_DIR);

        GcSnapshot gcBefore = GcSnapshot.take();

        CacheManager.initialize(
                CacheManager.builder(CACHE_DIR)
                        .shardCapacity(cfg.shardCapacity)
                        .memoryCacheSize(cfg.memoryCacheSize)
                        .useChronicleMap(cfg.useChronicleMap)
                        .indexShardCount(cfg.indexShardCount)
        );

        CacheDefinition<String> def = CacheDefinition.<String>builder()
                .name(CACHE_NAME)
                .supplier(SizedSupplier.of(() -> generateRows(cfg), cfg.size))
                .keyExtractor(CacheRow::getKey)
                .serializer(CacheDefinition.defaultSerializer())
                .deserializer(CacheDefinition.defaultDeserializer(s -> s))
                .ttl(Duration.ofHours(1))
                .build();

        CacheManager.register(def);
        validate(cfg);

        // ── WARMUP ───────────────────────────────────────────────────────────
        System.out.printf("  warmup %ds (%d thread)... ", WARMUP_SECONDS, cfg.threads);
        System.out.flush();
        runPhase(cfg, WARMUP_SECONDS);   // sonuç kullanılmıyor
        System.out.println("ok");

        // ── MEASUREMENT ──────────────────────────────────────────────────────
        System.out.printf("  measure %ds (%d thread)... ", MEASURE_SECONDS, cfg.threads);
        System.out.flush();
        PhaseResult read = runPhase(cfg, MEASURE_SECONDS);
        System.out.printf("ok  ops=%d  avg=%.2fµs  p99=%.2fµs  tps=%.0f%n",
                read.ops,
                read.stats.avgNs / 1_000.0,
                read.stats.p99Ns / 1_000.0,
                read.stats.throughputOpsPerSec);

        // ── RELOAD ───────────────────────────────────────────────────────────
        System.out.printf("  reload x%d... ", RELOAD_REPS);
        System.out.flush();
        long[] reloadNs = new long[RELOAD_REPS];
        for (int i = 0; i < RELOAD_REPS; i++) {
            long t0 = System.nanoTime();
            CacheManager.reload(CACHE_NAME);
            reloadNs[i] = System.nanoTime() - t0;
        }
        Stats reloadStats = Stats.compute(reloadNs, RELOAD_REPS, RELOAD_REPS * /* ortalama ms */ 1);
        System.out.printf("ok  avg=%.1fms%n", reloadStats.avgNs / 1_000_000.0);

        // ── GC / heap bitiş ──────────────────────────────────────────────────
        GcSnapshot gcAfter = GcSnapshot.take();
        long diskSize = dirSize(CACHE_DIR);
        long chronicleSize = chronicleSize(CACHE_DIR, cfg.useChronicleMap);

        // ── KAYDETME SIRASI: önce veri, sonra checkpoint ─────────────────────
        Csv.appendResult(cfg, read.stats, reloadStats, read.ops);
        Csv.appendMetrics(cfg, gcBefore, gcAfter, diskSize, chronicleSize);

        // ── Kapat ────────────────────────────────────────────────────────────
//        try {
//            CacheManager.shutdown();
//        } catch (Exception ignored) {
//        }

        // ── Checkpoint en son ────────────────────────────────────────────────
        Checkpoint.markDone(cfg.key());
    }

    // =========================================================================
    // PHASE — ISITMA veya ÖLÇÜM
    // =========================================================================

    /**
     * cfg.threads adet thread'i aynı anda başlatır, durationSecs boyunca çalıştırır.
     * Her thread kendi örnek dizisine yazar (lock yok).
     * Süre dolunca tüm thread örnekleri birleştirilir ve Stats hesaplanır.
     */
    static PhaseResult runPhase(Config cfg, int durationSecs) throws Exception {
        int threads = cfg.threads;
        long endNs = System.nanoTime() + durationSecs * 1_000_000_000L;
        AtomicBoolean running = new AtomicBoolean(true);

        // Her thread'in kendi örnek dizisi — paylaşımlı bellek yok
        long[][] threadSamples = new long[threads][SAMPLES_PER_THREAD];
        long[] threadCounts = new long[threads];
        long[] threadOps = new long[threads];
        long[] threadErrors = new long[threads];

        CyclicBarrier barrier = new CyclicBarrier(threads + 1); // +1 = main thread
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    barrier.await(); // tüm thread'ler hazır olana kadar bekle
                } catch (Exception e) {
                    return;
                }

                long[] samples = threadSamples[tid];
                long sampleIdx = 0;
                long ops = 0;
                long errors = 0;
                Random rng = new Random(tid * 31L + System.nanoTime());

                while (running.get()) {
                    int idx = nextIndex(cfg, rng);
                    String key = nextKey(cfg, idx, rng);
                    byte[] waste = cfg.allocateGarbage ? new byte[4096] : null;

                    long t0 = System.nanoTime();
                    String val = CacheManager.get(CACHE_NAME, key);
                    long ns = System.nanoTime() - t0;

                    ops++;

                    // Doğruluk kontrolü
                    if (key.startsWith("key-")) {
                        if (val == null || !val.equals(generateValue(cfg, idx))) errors++;
                    }

                    // Reservoir sampling (thread-local)
                    if (sampleIdx < SAMPLES_PER_THREAD) {
                        samples[(int) sampleIdx] = ns;
                        sampleIdx++;
                    } else {
                        // Knuth reservoir: eski bir yeri olasılıksal üzerine yaz
                        long r = (rng.nextLong() & Long.MAX_VALUE) % (sampleIdx + 1);
                        if (r < SAMPLES_PER_THREAD) samples[(int) r] = ns;
                        sampleIdx++;
                    }

                    // GC basıncı simülasyonu
                    if (waste != null) sink(waste);
                }

                threadCounts[tid] = Math.min(sampleIdx, SAMPLES_PER_THREAD);
                threadOps[tid] = ops;
                threadErrors[tid] = errors;
            });
        }

        // Tüm thread'leri aynı anda başlat
        barrier.await();
        long phaseStart = System.nanoTime();

        // Süre dolunca durdur
        Thread.sleep(durationSecs * 1_000L);
        running.set(false);

        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        long phaseDurationNs = System.nanoTime() - phaseStart;
        long totalOps = Arrays.stream(threadOps).sum();
        long totalErrors = Arrays.stream(threadErrors).sum();

        if (totalErrors > 0) {
            throw new IllegalStateException("Veri tutarsızlığı! errors=" + totalErrors);
        }

        // Thread örneklerini birleştir
        int totalSamples = (int) Arrays.stream(threadCounts).sum();
        long[] merged = new long[totalSamples];
        int offset = 0;
        for (int t = 0; t < threads; t++) {
            int cnt = (int) threadCounts[t];
            System.arraycopy(threadSamples[t], 0, merged, offset, cnt);
            offset += cnt;
        }

        Stats stats = Stats.compute(merged, totalSamples, phaseDurationNs);
        return new PhaseResult(totalOps, phaseDurationNs, stats);
    }

    // =========================================================================
    // ERİŞİM DESENİ
    // =========================================================================

    static int nextIndex(Config cfg, Random rng) {
        if ("HOT_80_20".equals(cfg.accessPattern) && rng.nextInt(10) < 8) {
            return rng.nextInt(Math.max(1, cfg.size / 10));
        }
        return rng.nextInt(cfg.size);
    }

    static String nextKey(Config cfg, int idx, Random rng) {
        return (rng.nextInt(100) < cfg.hitRate) ? "key-" + idx : "missing-" + idx;
    }

    // =========================================================================
    // VERİ ÜRETME & DOĞRULAMA
    // =========================================================================

    static Stream<CacheRow> generateRows(Config cfg) {
        return IntStream.range(0, cfg.size)
                .mapToObj(i -> new CacheRow(CACHE_NAME, "key-" + i, generateValue(cfg, i)));
    }

    static String generateValue(Config cfg, int i) {
        int base = Math.max(10, cfg.valueSize);
        return "v".repeat(base - 10) + i;
    }

    static void validate(Config cfg) {
        Random rng = new Random(42);
        for (int i = 0; i < 1_000; i++) {
            int idx = rng.nextInt(cfg.size);
            String key = "key-" + idx;
            String expected = generateValue(cfg, idx);
            String actual = CacheManager.get(CACHE_NAME, key);
            if (!expected.equals(actual)) {
                throw new IllegalStateException(
                        "Validasyon hatası: key=" + key +
                                " expected=" + expected + " actual=" + actual);
            }
        }
    }

    // =========================================================================
    // CONFIG
    // =========================================================================

    static class Config {
        final int size;
        final int shardCapacity;
        final int indexShardCount;
        final String accessPattern;
        final int hitRate;
        final int valueSize;
        final int memoryCacheSize;
        final boolean allocateGarbage;
        final boolean useChronicleMap;
        final int threads;

        Config(int size, int shardCapacity, int indexShardCount, String accessPattern,
               int hitRate, int valueSize, int memoryCacheSize,
               boolean allocateGarbage, boolean useChronicleMap, int threads) {
            this.size = size;
            this.shardCapacity = shardCapacity;
            this.indexShardCount = indexShardCount;
            this.accessPattern = accessPattern;
            this.hitRate = hitRate;
            this.valueSize = valueSize;
            this.memoryCacheSize = memoryCacheSize;
            this.allocateGarbage = allocateGarbage;
            this.useChronicleMap = useChronicleMap;
            this.threads = threads;
        }

        String key() {
            return String.join("_",
                    String.valueOf(size),
                    String.valueOf(shardCapacity),
                    String.valueOf(indexShardCount),
                    accessPattern,
                    String.valueOf(hitRate),
                    String.valueOf(valueSize),
                    String.valueOf(memoryCacheSize),
                    String.valueOf(allocateGarbage),
                    String.valueOf(useChronicleMap),
                    "t" + threads
            );
        }
    }

    // =========================================================================
    // PHASE RESULT
    // =========================================================================

    static class PhaseResult {
        final long ops;
        final long durationNs;
        final Stats stats;

        PhaseResult(long ops, long durationNs, Stats stats) {
            this.ops = ops;
            this.durationNs = durationNs;
            this.stats = stats;
        }
    }

    // =========================================================================
    // STATS — tüm hesaplamalar burada
    // =========================================================================

    static class Stats {
        final long avgNs;
        final long p50Ns;
        final long p90Ns;
        final long p99Ns;
        final long p999Ns;
        final double throughputOpsPerSec;

        Stats(long avgNs, long p50Ns, long p90Ns, long p99Ns, long p999Ns, double tps) {
            this.avgNs = avgNs;
            this.p50Ns = p50Ns;
            this.p90Ns = p90Ns;
            this.p99Ns = p99Ns;
            this.p999Ns = p999Ns;
            this.throughputOpsPerSec = tps;
        }

        static Stats compute(long[] samples, int count, long durationNs) {
            if (count == 0) return new Stats(0, 0, 0, 0, 0, 0);

            long sum = 0;
            for (int i = 0; i < count; i++) sum += samples[i];
            long avgNs = sum / count;

            long[] sorted = Arrays.copyOf(samples, count);
            Arrays.sort(sorted);

            long p50 = sorted[(int) (count * 0.500)];
            long p90 = sorted[(int) (count * 0.900)];
            long p99 = sorted[(int) (count * 0.990)];
            long p999 = sorted[(int) Math.min(count * 0.999, count - 1)];

            double tps = durationNs > 0
                    ? count / (durationNs / 1_000_000_000.0)
                    : 0;

            return new Stats(avgNs, p50, p90, p99, p999, tps);
        }
    }

    // =========================================================================
    // GC SNAPSHOT
    // =========================================================================

    static class GcSnapshot {
        final long gcCount;
        final long gcTimeMs;
        final long heapUsedBytes;

        GcSnapshot(long gcCount, long gcTimeMs, long heapUsedBytes) {
            this.gcCount = gcCount;
            this.gcTimeMs = gcTimeMs;
            this.heapUsedBytes = heapUsedBytes;
        }

        static GcSnapshot take() {
            List<GarbageCollectorMXBean> beans = ManagementFactory.getGarbageCollectorMXBeans();
            long count = beans.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
            long time = beans.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
            Runtime rt = Runtime.getRuntime();
            long heap = rt.totalMemory() - rt.freeMemory();
            return new GcSnapshot(count, time, heap);
        }

        long gcCountDelta(GcSnapshot before) {
            return gcCount - before.gcCount;
        }

        long gcTimeDelta(GcSnapshot before) {
            return gcTimeMs - before.gcTimeMs;
        }

        long heapDelta(GcSnapshot before) {
            return heapUsedBytes - before.heapUsedBytes;
        }
    }

    // =========================================================================
    // CHECKPOINT
    // =========================================================================

    static class Checkpoint {

        static boolean isDone(String key) {
            return Files.exists(doneFile(key));
        }

        /**
         * Atomik write: önce .tmp, sonra ATOMIC_MOVE → .done
         * Yarım kalan dosya "tamamlandı" gibi algılanmaz.
         */
        static void markDone(String key) {
            Path target = doneFile(key);
            Path tmp = CHECKPOINT_DIR.resolve(key + ".tmp");
            try {
                Files.writeString(tmp, key + "\n", StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                Files.move(tmp, target,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                System.err.printf("[CHECKPOINT] İşaretlenemedi: %s — %s%n", key, e.getMessage());
            }
        }

        private static Path doneFile(String key) {
            return CHECKPOINT_DIR.resolve(key + ".done");
        }
    }

    // =========================================================================
    // CSV YAZMA
    // =========================================================================

    static class Csv {

        // ── Results CSV ──────────────────────────────────────────────────────

        static final String RESULT_HEADER = String.join(",",
                "timestamp", "key",
                "size", "shardCapacity", "indexShardCount", "accessPattern",
                "hitRate", "valueSize", "memoryCacheSize", "allocateGarbage",
                "useChronicleMap", "threads",
                "ops",
                "avgNs", "p50Ns", "p90Ns", "p99Ns", "p999Ns",
                "throughputOpsPerSec",
                "reloadAvgMs", "reloadP99Ms"
        );

        static void appendResult(Config cfg, Stats read, Stats reload, long ops) {
            synchronized (WRITE_LOCK) {
                boolean needHeader = !Files.exists(RESULTS_CSV);
                try (BufferedWriter w = Files.newBufferedWriter(RESULTS_CSV, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                    if (needHeader) {
                        w.write(RESULT_HEADER);
                        w.newLine();
                    }

                    w.write(Instant.now().toString());
                    w.write("," + cfg.key());
                    // Parametre sütunları (post-processing için ayrı ayrı)
                    w.write("," + cfg.size);
                    w.write("," + cfg.shardCapacity);
                    w.write("," + cfg.indexShardCount);
                    w.write("," + cfg.accessPattern);
                    w.write("," + cfg.hitRate);
                    w.write("," + cfg.valueSize);
                    w.write("," + cfg.memoryCacheSize);
                    w.write("," + cfg.allocateGarbage);
                    w.write("," + cfg.useChronicleMap);
                    w.write("," + cfg.threads);
                    // Sonuçlar
                    w.write("," + ops);
                    w.write("," + read.avgNs);
                    w.write("," + read.p50Ns);
                    w.write("," + read.p90Ns);
                    w.write("," + read.p99Ns);
                    w.write("," + read.p999Ns);
                    w.write(String.format(Locale.US, ",%.1f", read.throughputOpsPerSec));
                    // Reload (ms cinsinden — reload saniyeler mertebesinde)
                    w.write(String.format(Locale.US, ",%.3f", reload.avgNs / 1_000_000.0));
                    w.write(String.format(Locale.US, ",%.3f", reload.p99Ns / 1_000_000.0));
                    w.newLine();
                } catch (IOException e) {
                    System.err.printf("[CSV] Sonuç yazılamadı: %s%n", e.getMessage());
                }
            }
        }

        // ── Metrics CSV ──────────────────────────────────────────────────────

        static final String METRICS_HEADER = String.join(",",
                "timestamp", "key",
                "heapUsedBefore", "heapUsedAfter", "heapMax",
                "gcCountDelta", "gcTimeDeltaMs",
                "diskSizeBytes", "chronicleMapBytes"
        );

        static void appendMetrics(Config cfg,
                                  GcSnapshot before, GcSnapshot after,
                                  long diskSize, long chronicleSize) {
            synchronized (WRITE_LOCK) {
                boolean needHeader = !Files.exists(METRICS_CSV);
                try (BufferedWriter w = Files.newBufferedWriter(METRICS_CSV, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                    if (needHeader) {
                        w.write(METRICS_HEADER);
                        w.newLine();
                    }

                    w.write(Instant.now().toString());
                    w.write("," + cfg.key());
                    w.write("," + before.heapUsedBytes);
                    w.write("," + after.heapUsedBytes);
                    w.write("," + Runtime.getRuntime().maxMemory());
                    w.write("," + after.gcCountDelta(before));
                    w.write("," + after.gcTimeDelta(before));
                    w.write("," + diskSize);
                    w.write("," + chronicleSize);
                    w.newLine();
                } catch (IOException e) {
                    System.err.printf("[CSV] Metrik yazılamadı: %s%n", e.getMessage());
                }
            }
        }
    }

    // =========================================================================
    // DOSYA SİSTEMİ YARDIMCILARI
    // =========================================================================

    static long dirSize(Path dir) {
        if (!Files.exists(dir)) return 0;
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0;
                        }
                    })
                    .sum();
        } catch (IOException e) {
            return 0;
        }
    }

    static long chronicleSize(Path dir, boolean enabled) {
        if (!enabled || !Files.exists(dir)) return 0;
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(p -> p.toString().endsWith(".cfs"))
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0;
                        }
                    })
                    .sum();
        } catch (IOException e) {
            return 0;
        }
    }

    static void deleteDir(Path dir) {
        CacheManager.purgeStaleVersions(dir);
    }

    static void deleteDirectoryRecursively(Path dir) throws IOException {
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            System.err.println("Failed to delete: " + p + " – " + e.getMessage());
                        }
                    });
        }
    }

    static void mkdir(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Dizin oluşturulamadı: " + dir, e);
        }
    }

    // =========================================================================
    // YARDIMCI — GC BASKISINI GERÇEK KIL
    // =========================================================================

    /**
     * JIT'in waste array'ini optimize etmesini engeller
     */
    static volatile long blackhole = 0;

    static void sink(byte[] arr) {
        blackhole ^= arr[0];
    }
}