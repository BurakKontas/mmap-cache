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

## Example: initialize `CacheManager` (recommended via builder)

The preferred way to initialize the manager is with the builder. This shows the important knobs we added: shard
capacity, overall memory-cache default, and Chronicle `averageKey` configuration.

```java
import tr.kontas.cache.CacheManager;
import java.nio.file.Path;

Path base = Path.of("/var/lib/mycache");
CacheManager.Builder builder = CacheManager.builder(base)
    .shardCapacity(100_000)         // how many records per shard file (shard file capacity)
    .memoryCacheSize(10_000)        // manager-wide default for CacheDefinition.memoryCacheMaxSize (0 = disabled)
    .defaultMaxKeyBytes(64)
    .defaultMaxValueBytes(512)
    .chronicleAverageKey("key-00000000"); // average key template used when building a ChronicleMap index

CacheManager.initialize(builder);
```

Notes:

- `memoryCacheSize` is a manager-wide default that will be applied to definitions which do not set their own
  `memoryCacheMaxSize` (only when `memoryCacheMaxSize == 0`).
- If you prefer to rely entirely on ChronicleMap (off-heap index + no heap L1 cache), set `memoryCacheSize(0)` and use
  Chronicle for the index.

---

## Example: `CacheDefinition` with Caffeine options

The `CacheDefinition` builder exposes the Caffeine-related options you requested: `memoryCacheMaxSize`,
`memoryCacheTtl` (expire-after-write), and `memoryCacheIdleTtl` (expire-after-access).

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

Behavior notes:

- If `memoryCacheMaxSize <= 0`, no Caffeine cache is created and reads go directly to the mmap shards.
- If `memoryCacheTtl` is non-null and non-zero, the Caffeine builder will apply `expireAfterWrite(Duration)`.
- If `memoryCacheIdleTtl` is non-null and non-zero, the Caffeine builder will apply `expireAfterAccess(Duration)`.

---

## How disk size is calculated (record layout)

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

The index (ConcurrentHashMap or ChronicleMap) consumes additional RAM or disk depending on the choice.

---

## ChronicleMap & tuning tips

- Use `averageKey()` / `averageValueSize()` / `entries()` to pre-size ChronicleMap for your expected dataset to avoid
  resizing.
- `chronicleAverageKey` on the manager builder is a convenience to set a reproducible average key template (used by our
  builder when creating a ChronicleMap index).
- ChronicleMap keeps the index off-heap which is recommended for very large keysets (tens of millions of keys).

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

A minimal PowerShell snippet for folder size is included in the project README (see **Measuring current cache folder
size**).

Notes that affect results:

- `maxKeyBytes` / `maxValueBytes` (record size) — the single biggest factor
- `shardCapacity` (smaller files may be preferable if your access set is very small)
- storage medium (NVMe vs HDD) and OS page-cache behavior
- serializer/deserializer CPU cost

---

## CI Release (GitHub Actions)

This project includes a `release.yml` workflow that runs a Java matrix and deploys to Maven Central on tagged releases.
Important points in the workflow:

- It tests across multiple JDKs (example matrix includes 11, 17, 21, 25).
- GPG import, Maven settings, and the actual `mvn deploy` step are gated to the Java 11 job to keep signing consistent.
- Ensure repository secrets are configured: `MAVEN_USERNAME`, `MAVEN_PASSWORD`, `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`.

If you want releases to run with a different JDK distribution (eg. Corretto instead of Temurin) edit
`.github/workflows/release.yml` `uses: actions/setup-java@v4` distribution or use a custom runner with the desired JDK
installed.

---

## License

This repository is licensed under GNU GPL v3.0 (see `LICENSE`).

---

If you'd like I can also:

- add a quick `docs/performance.md` with exact commands used to measure and parse results,
- add a small `perf-runner` class that writes CSV results (time, bytes, p50/p95/p99) to the `target/` folder for
  automated runs.
