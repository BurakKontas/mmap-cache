package tr.kontas.cache;

import lombok.Getter;

import java.util.Objects;


@Getter
public final class CacheRow {
    private final String tableName;
    private final String key;
    private final Object value;
    private final long fetchedAt;

    public CacheRow(String key, Object value) {
        this(null, key, value);
    }

    public CacheRow(String tableName, String key, Object value) {
        this.tableName = tableName;
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.value = value;
        this.fetchedAt = System.currentTimeMillis();
    }

    @SuppressWarnings("unchecked")
    public <T> T getValue() {
        return (T) value;
    }

    public String getValueAsString() {
        if (value instanceof String) {
            return (String) value;
        }
        return value != null ? value.toString() : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CacheRow)) return false;
        CacheRow other = (CacheRow) o;
        return Objects.equals(key, other.key) &&
                Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, key);
    }

    @Override
    public String toString() {
        return "CacheRow{table='" + tableName + "', key='" + key + "', value=" + value + '}';
    }
}