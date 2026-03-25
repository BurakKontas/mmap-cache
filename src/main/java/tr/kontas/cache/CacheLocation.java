package tr.kontas.cache;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;

/**
 * Shard index'inde bir kaydın fiziksel konumunu tutar.
 * <p>
 * Chronicle Map entegrasyonu için {@link BytesMarshallable} implement edilmiştir.
 * Chronicle bu interface aracılığıyla nesneyi off-heap'e serileştirir;
 * JVM heap'inde hiçbir alan kaplamaz.
 * <p>
 * Serileştirilmiş boyut: int(4) + int(4) = 8 byte / kayıt
 */
public final class CacheLocation implements BytesMarshallable {

    private int shardId;
    private int offset;

    /**
     * Chronicle Map'in kendi içinde nesne oluşturabilmesi için
     * no-arg constructor zorunludur.
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