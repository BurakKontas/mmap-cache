package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheManager;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CacheManagerBuilderTests {

    @Test
    void chronicleAverageKey_nullOrEmpty_throws() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        CacheManager.Builder b = CacheManager.builder(tmp);
        assertThrows(IllegalArgumentException.class, () -> b.chronicleAverageKey(null));
        assertThrows(IllegalArgumentException.class, () -> b.chronicleAverageKey(""));
    }

    @Test
    void indexShardCount_lessThanOne_throws() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        CacheManager.Builder b = CacheManager.builder(tmp);
        assertThrows(IllegalArgumentException.class, () -> b.indexShardCount(0));
        assertThrows(IllegalArgumentException.class, () -> b.indexShardCount(-2));
    }

    @Test
    void memoryCacheSize_negative_throws() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        CacheManager.Builder b = CacheManager.builder(tmp);
        assertThrows(IllegalArgumentException.class, () -> b.memoryCacheSize(-1));
    }

    @Test
    void builder_basePath_null_throws() {
        assertThrows(IllegalArgumentException.class, () -> new CacheManager.Builder(null));
    }
}
