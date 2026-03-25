package net.openhft.chronicle.map;

import java.io.Closeable;
import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test-only stub to avoid pulling the real ChronicleMap classes into unit tests when CI environments
 * have version mismatches. Returns a lightweight Map implementation that also implements Closeable
 * and throws if accessed after close(), matching the expectations in integration tests.
 */
public final class ChronicleMapBuilder<K, V> {

    public static <K, V> ChronicleMapBuilder<K, V> of(Class<K> keyClass, Class<V> valueClass) {
        return new ChronicleMapBuilder<>();
    }

    public ChronicleMapBuilder<K, V> name(String name) {
        return this;
    }

    public ChronicleMapBuilder<K, V> averageKey(String avg) {
        return this;
    }

    // Some Chronicle versions expose averageKey(Object) (generic). Provide overload to match runtime.
    public ChronicleMapBuilder<K, V> averageKey(Object avg) {
        return this;
    }

    public ChronicleMapBuilder<K, V> entries(long entries) {
        return this;
    }

    public ChronicleMapBuilder<K, V> averageValueSize(int v) {
        return this;
    }

    // Some Chronicle versions accept a double for average value size; provide an overload.
    public ChronicleMapBuilder<K, V> averageValueSize(double v) {
        return this;
    }

    public Map<K, V> createPersistedTo(File file) {
        // Attempt to create the parent directories and the file to mimic ChronicleMap's persisted-to behavior.
        try {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            if (!file.exists()) file.createNewFile();
        } catch (Exception ignored) {
        }
        // Return a lightweight Closeable map used for tests. This map will throw on access after close().
        return new TestChronicleMap<>();
    }

    // Lightweight test double that behaves like a Closeable ChronicleMap: after close(), access throws.
    public static final class TestChronicleMap<K, V> extends ConcurrentHashMap<K, V> implements Closeable, Map<K, V> {
        private volatile boolean closed = false;

        @Override
        public synchronized void close() {
            this.closed = true;
        }

        private void ensureOpen() {
            if (closed) throw new IllegalStateException("ChronicleMap (test stub) is closed");
        }

        @Override
        public V put(K key, V value) {
            ensureOpen();
            return super.put(key, value);
        }

        @Override
        public V get(Object key) {
            ensureOpen();
            return super.get(key);
        }

        @Override
        public V remove(Object key) {
            ensureOpen();
            return super.remove(key);
        }

        @Override
        public void clear() {
            ensureOpen();
            super.clear();
        }
    }
}
