package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import tr.kontas.cache.CacheRow;
import tr.kontas.cache.SizedSupplier;

import java.util.OptionalLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SizedSupplierTests {

    @Test
    void defaultCount_isEmpty_forUnknownSupplier() {
        SizedSupplier s = SizedSupplier.unknown(() -> Stream.of(new CacheRow("k1", "v1")));
        OptionalLong c = s.count();
        assertEquals(OptionalLong.empty(), c);
        try (Stream<CacheRow> st = s.get()) {
            assertEquals("k1", st.findFirst().map(CacheRow::getKey).orElse(null));
        }
    }

    @Test
    void of_withKnownCount_returnsCountAndStream() {
        SizedSupplier s = SizedSupplier.of(() -> Stream.of(
                new CacheRow("k1", "v1"),
                new CacheRow("k2", "v2")
        ), 2L);

        OptionalLong c = s.count();
        assertTrue(c.isPresent());
        assertEquals(2L, c.getAsLong());

        try (Stream<CacheRow> st = s.get()) {
            assertEquals(2, st.count());
        }
    }

    @Test
    void of_negativeCount_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> SizedSupplier.of(Stream::empty, -1L));
    }

    @Test
    void unknown_supplier_getsDelegated() {
        Stream<CacheRow> supplierStream = Stream.of(new CacheRow("kx", "vx"));
        SizedSupplier s = SizedSupplier.unknown(() -> supplierStream);
        try (Stream<CacheRow> st = s.get()) {
            assertEquals("kx", st.findFirst().map(CacheRow::getKey).orElse(null));
        }
    }
}
