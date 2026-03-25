package tr.kontas.cache;

import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Stream supplier with optional row-count metadata.
 * <p>
 * Providing count upfront lets the reload pipeline skip the count pass
 * and pre-size shards/indexes more efficiently.
 */
@FunctionalInterface
public interface SizedSupplier extends Supplier<Stream<CacheRow>> {

    /**
     * Returns known row count, if available.
     *
     * @return known row count or {@link OptionalLong#empty()}
     */
    default OptionalLong count() {
        return OptionalLong.empty();
    }

    // ── Factory methods ───────────────────────────────────────────────────────

    /**
     * Creates a sized supplier with known row count.
     *
     * @param supplier underlying stream supplier
     * @param count    known row count
     * @return sized supplier instance
     */
    static SizedSupplier of(Supplier<Stream<CacheRow>> supplier, long count) {
        if (count < 0) throw new IllegalArgumentException("count cannot be negative: " + count);
        return new SizedSupplier() {
            @Override
            public Stream<CacheRow> get() {
                return supplier.get();
            }

            @Override
            public OptionalLong count() {
                return OptionalLong.of(count);
            }
        };
    }

    /**
     * Creates a sized supplier with unknown row count.
     *
     * @param supplier underlying stream supplier
     * @return sized supplier instance without count hint
     */
    static SizedSupplier unknown(Supplier<Stream<CacheRow>> supplier) {
        return supplier::get;
    }
}