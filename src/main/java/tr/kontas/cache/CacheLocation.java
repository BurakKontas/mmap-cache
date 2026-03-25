package tr.kontas.cache;

public final class CacheLocation {
    private final int shardId;
    private final int offset;

    public CacheLocation(int shardId, int offset) {
        this.shardId = shardId;
        this.offset = offset;
    }

    public int shardId() {
        return shardId;
    }

    public int offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheLocation that = (CacheLocation) o;
        return shardId == that.shardId && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(shardId, offset);
    }

    @Override
    public String toString() {
        return "CacheLocation[shardId=" + shardId + ", offset=" + offset + "]";
    }
}