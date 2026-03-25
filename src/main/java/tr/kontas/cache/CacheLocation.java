package tr.kontas.cache;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;

/**
 * Stores the physical location of a cached record in shard storage.
 * <p>
 * Implements {@link BytesMarshallable} for Chronicle Map integration.
 * Chronicle serializes this object off-heap through this contract,
 * minimizing on-heap footprint.
 * <p>
 * Serialized size: int(4) + int(4) = 8 bytes per record.
 */
public final class CacheLocation implements BytesMarshallable {

    private int shardId;
    private int offset;

    /**
     * Required no-arg constructor for Chronicle Map internal object creation.
     */
    public CacheLocation() {
    }

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

    // ── BytesMarshallable ───────────────────────────────────────────────────

    @Override
    public void writeMarshallable(BytesOut<?> bytes) {
        bytes.writeInt(shardId);
        bytes.writeInt(offset);
    }

    @Override
    public void readMarshallable(BytesIn<?> bytes) {
        shardId = bytes.readInt();
        offset = bytes.readInt();
    }

    // ── Object ──────────────────────────────────────────────────────────────

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