# High Performance Memory-Mapped Cache

A compact, production-ready Java caching library designed to store and serve very large key-value datasets with minimal
heap pressure. Data is written as fixed-size binary records to memory-mapped shard files (mmap) while an optional
in-heap Caffeine layer serves hot keys. An off-heap ChronicleMap index can be used for very large key sets.

This README explains quick usage (builder + definition), sizing, performance considerations, and CI release notes.

---

## Quick overview

- On-disk storage uses fixed-size records inside per-cache shards.
- Optionally use a Caffeine in-memory L1 cache for hot keys (configurable size and TTLs).
- Optionally use ChronicleMap as an off-heap persistent index (String -> CacheLocation).
- Designed for low latency and predictable IO patterns (O(1) seek per lookup).

---

## Requirements

- Primary development/CI target: **JDK 11** (the project `pom.xml` uses `<maven.compiler.release>11</maven.compiler.release>`).
- Tests can run on newer JDKs (17, 21, 25) but note the compatibility caveats in Troubleshooting below.
- Maven 3.6+ to build and run tests.

---

## Quick usage examples

(kept from original README: manager builder + definition examples)

### Initialize `CacheManager` (recommended via builder)

```java
import tr.kontas.cache.CacheManager;
import java.nio.file.Path;

Path base = Path.of("/var/lib/mycache");
CacheManager.Builder builder = CacheManager.builder(base)
    .shardCapacity(100_000)         // how many records per data shard file
    .memoryCacheSize(10_000)        // manager-wide default for CacheDefinition.memoryCacheMaxSize (0 = disabled)
    .defaultMaxKeyBytes(64)
    .defaultMaxValueBytes(512)
    .chronicleAverageKey("key-00000000") // average key template used when building a ChronicleMap index
    .indexShardCount(16);           // splits the ChronicleMap index into N shards to bypass OS mmap limits (e.g. 4GB limit on Windows)

CacheManager.initialize(builder);

// Retrieve from cache
String value = CacheManager.get("myCache", "some-key");
```

### `CacheDefinition` with Caffeine options

```java
import tr.kontas.cache.CacheDefinition;
import tr.kontas.cache.CacheRow;
import java.time.Duration;
import java.util.stream.Stream;

CacheDefinition<String> def = CacheDefinition.<String>builder()
    .name("myCache")
    .supplier(() -> Stream.of(new CacheRow("k","v")))
    .keyExtractor(CacheRow::getKey)
    .serializer(CacheDefinition.defaultSerializer())
    .deserializer(CacheDefinition.defaultDeserializer(s -> s))
    .memoryCacheMaxSize(10_000)             // how many entries in the L1 Caffeine cache (0=disabled)
    .memoryCacheTtl(Duration.ofMinutes(5))  // expire-after-write (optional)
    .memoryCacheIdleTtl(Duration.ofMinutes(2)) // expire-after-access (optional)
    .ttl(Duration.ofHours(1))               // full cache reload TTL (background reload)
    .dynamicSizing(true)                    // calculate per-record sizes from the provided supplier
    .build();

CacheManager.register(def);
```

---

## Disk layout & sizing

Each record uses a fixed layout:

- id: 8 bytes (long)
- keyLen: 2 bytes (short)
- key: maxKeyBytes bytes
- valueLen: 2 bytes (short)
- value: maxValueBytes bytes
- timestamp: 8 bytes (long)

```
recordSize = 8 + 2 + maxKeyBytes + 2 + maxValueBytes + 8
```

```
shardFileSize = recordSize * shardCapacity
totalDiskBytes ≈ shardFileSize * shardCount
```

Example: `maxKeyBytes=32`, `maxValueBytes=128` → recordSize = 180 bytes. With 1,000,000 rows and
`shardCapacity=100_000` → 10 shards × 100k × 180 ≈ 172 MB.

---

## ChronicleMap Index Sharding (Windows compatibility)

To support Windows environments where a single memory-mapped file cannot exceed 4GB (~4096 MiB), the `ChronicleMap` index is horizontally partitioned into multiple files (e.g., `index_0000.chm`, `index_0001.chm`).
Keys are deterministically routed to a specific shard using their hash code (`(key.hashCode() & 0x7fffffff) % indexShardCount`).

- Set `indexShardCount` on the `CacheManager.Builder` based on your total row count to keep individual index shards under the 4GB limit.
- Default `indexShardCount` is 16.

---

## ChronicleMap & tuning tips

- Use `averageKey()` / `averageValueSize()` / `entries()` to pre-size ChronicleMap for your expected dataset to avoid
  resizing.
- `chronicleAverageKey` on the manager builder is a convenience to set a reproducible average key template (used by our
  builder when creating a ChronicleMap index).
- ChronicleMap keeps the index off-heap which is recommended for very large keysets (tens of millions of keys).

---

## Build, Test & Coverage (commands)

From the project root:

- Build + run tests:

```bash
mvn clean install
```

- Run only tests:

```bash
mvn test
```

- Run a single test class (useful when iterating):

```bash
mvn -Dtest=tr.kontas.cache.unit.CacheTests test
```

- Generate JaCoCo coverage report:

```bash
mvn test jacoco:report
# Open the report: target/site/jacoco/index.html
```

If you need to run tests from IntelliJ or another IDE under a newer JDK and you see Byte Buddy / Mockito errors,
include these VM options in the test runner:

```
-Dnet.bytebuddy.experimental=true -XX:+EnableDynamicAgentLoading
```

(See Troubleshooting below for more details.)

---

## CI / Release notes

- Some CI workflows activate a `release` profile (for packaging/deploy). If your CI uses `-Prelease`, ensure your
  `distributionManagement` is configured with a repository or set `maven.deploy.skip=true` to avoid an attempted deploy.
- This repository includes a `release` profile placeholder in `pom.xml` which sets `maven.deploy.skip=true` by default to
  avoid accidental deploys in PR/CI runs. If you want the CI to deploy, replace or override the profile to provide real
  registry configuration.

Example: skipping deploy in CI for dry-run releases

```bash
mvn -Prelease -DskipTests install
```

To actually deploy, configure `distributionManagement` in `pom.xml` or use `-DaltDeploymentRepository` with
`mvn deploy`.

---

## Troubleshooting

1. Byte Buddy / Mockito inline errors on newer JDKs (e.g. Java 25):
   - Symptom: stack-trace mentioning Byte Buddy, "Java 25 (69) is not supported...", or Mockito unable to inline mock classes.
   - Workaround: run tests with the experimental Byte Buddy flag and enable dynamic agent loading:

     ```bash
     mvn -DskipTests=false test -DargLine="-Dnet.bytebuddy.experimental=true -XX:+EnableDynamicAgentLoading"
     ```

   - Long-term fix: upgrade `net.bytebuddy` and `mockito-inline` to versions that officially support your JDK.

2. SLF4J "No providers were found" warning during tests:
   - Fix: ensure a test-scoped SLF4J implementation is available (e.g. `slf4j-simple` in `test` scope). The project includes
     a test-scoped `slf4j-simple` dependency to silence the warning.

3. Flaky teardown (`NoSuchFileException` in `@AfterEach`) on CI:
   - Cause: test temp folders/files can be removed concurrently or by other processes.
   - Mitigation: test teardown is tolerant of missing files (catch and ignore `NoSuchFileException` / `UncheckedIOException`).

---

## Performance testing and metrics (recommended harness)

We include a `CachePerformanceTest` harness. Recommended steps to measure performance on your hardware:

1. Pick representative dataset sizes (eg. 1k, 10k, 100k, 1M, 10M, 100M).
2. Run the write/load phase to produce shard files and index. Capture: time taken, resulting file sizes.
3. Run randomized reads over the keyspace and capture per-operation latencies (p50/p95/p99) and throughput.

Record the following metrics per run:

- write time (seconds)
- total disk bytes (MB/GB)
- read latencies: average, p50, p95, p99 (µs or ms)
- heap usage and GC statistics (before/after)


Notes that affect results:

- `maxKeyBytes` / `maxValueBytes` (record size) — the single biggest factor
- `shardCapacity` (smaller files may be preferable if your access set is very small)
- storage medium (NVMe vs HDD) and OS page-cache behavior
- serializer/deserializer CPU cost

---

## License

This repository is licensed under GNU GPL v3.0 (see `LICENSE`).
