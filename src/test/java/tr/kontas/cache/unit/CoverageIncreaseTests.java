package tr.kontas.cache.unit;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import tr.kontas.cache.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockStatic;

public class CoverageIncreaseTests {

    @Test
    void purgeStaleVersions_handles_files_list_throw() throws Exception {
        Path tmp = Files.createTempDirectory("ci_purge_list_throw");
        CacheManager.initialize(tmp);

        // create a version dir
        Path vdir = tmp.resolve("v1");
        Files.createDirectories(vdir);
        // create a version object to ensure manager has something, not used directly here
        new CacheVersion<>(vdir, new CacheShard[0], Map.of(), TestHelpers.simpleDefinition("v1", 0));

        // mock Files.list to throw when called inside purgeStaleVersions
        try (MockedStatic<Files> mocked = mockStatic(Files.class)) {
            mocked.when(() -> Files.list(tmp)).thenThrow(new RuntimeException("list fail"));
            // call purgeStaleVersions (private) via reflection on manager instance
            Field instF = CacheManager.class.getDeclaredField("INSTANCE");
            instF.setAccessible(true);
            Object mgr = instF.get(null);
            var m = CacheManager.class.getDeclaredMethod("purgeStaleVersions", java.nio.file.Path.class);
            m.setAccessible(true);
            assertDoesNotThrow(() -> m.invoke(mgr, tmp));
        }
    }

    @Test
    void cleanupOldVersion_handles_locked_file_delete() throws Exception {
        Path tmp = Files.createTempDirectory("ci_cleanup_locked");
        Path ver = tmp.resolve("v2");
        Files.createDirectories(ver);
        Path f = ver.resolve("locked.dat");
        Files.writeString(f, "x");

        CacheVersion<String> v = new CacheVersion<>(ver, new CacheShard[0], Map.of(), TestHelpers.simpleDefinition("v2", 0));
        CacheManager.initialize(tmp);

        Field instF = CacheManager.class.getDeclaredField("INSTANCE");
        instF.setAccessible(true);
        Object mgr = instF.get(null);

        // invoke cleanupOldVersion privately to exercise the delete-locked-file catch
        var cleanup = CacheManager.class.getDeclaredMethod("cleanupOldVersion", String.class, CacheVersion.class);
        cleanup.setAccessible(true);
        assertDoesNotThrow(() -> cleanup.invoke(mgr, "v2", v));
    }

    @Test
    void cacheShard_close_and_flush_nulls_handled() throws Exception {
        Path tmp = Files.createTempDirectory("ci_shard_close_nulls");
        int maxKey = 4, maxVal = 4;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        CacheShard shard = new CacheShard(tmp.resolve("s.dat"), recordSize, 1, maxKey, maxVal);

        // set internal buffer to null via reflection to force NPE inside flush (should be caught)
        Field bufF = CacheShard.class.getDeclaredField("buffer");
        bufF.setAccessible(true);
        bufF.set(shard, null);

        // set channel to a mock that will throw on write/close
        try (MockedStatic<FileChannel> mockedFileChannel = mockStatic(FileChannel.class)) {
            mockedFileChannel.when(() -> FileChannel.open(java.nio.file.Path.of(shard.getFilePath()), StandardOpenOption.READ, StandardOpenOption.WRITE)).thenThrow(new IOException("open fail"));
            // close the shard — should not throw
            assertDoesNotThrow(shard::close);
        }
    }

    @Test
    void purgeStaleVersions_handles_files_walk_throw() throws Exception {
        Path tmp = Files.createTempDirectory("ci_purge_walk_throw");
        CacheManager.initialize(tmp);
        try (MockedStatic<Files> mocked = mockStatic(Files.class)) {
            mocked.when(() -> Files.list(tmp)).thenReturn(Stream.of());
            mocked.when(() -> Files.walk(tmp)).thenThrow(new RuntimeException("walk fail"));
            Field instF = CacheManager.class.getDeclaredField("INSTANCE");
            instF.setAccessible(true);
            Object mgr = instF.get(null);
            var m = CacheManager.class.getDeclaredMethod("purgeStaleVersions", java.nio.file.Path.class);
            m.setAccessible(true);
            assertDoesNotThrow(() -> m.invoke(mgr, tmp));
        }
    }

    @Test
    void purgeStaleVersions_handles_deleteIfExists_false() throws Exception {
        Path tmp = Files.createTempDirectory("ci_purge_delete_false");
        Path v = tmp.resolve("v3");
        Files.createDirectories(v);
        CacheManager.initialize(tmp);
        try (MockedStatic<Files> mocked = mockStatic(Files.class)) {
            mocked.when(() -> Files.list(tmp)).thenReturn(Stream.of(v));
            mocked.when(() -> Files.walk(v)).thenReturn(Stream.of(v));
            mocked.when(() -> Files.deleteIfExists(v)).thenReturn(false);
            Field instF = CacheManager.class.getDeclaredField("INSTANCE");
            instF.setAccessible(true);
            Object mgr = instF.get(null);
            var m = CacheManager.class.getDeclaredMethod("purgeStaleVersions", java.nio.file.Path.class);
            m.setAccessible(true);
            assertDoesNotThrow(() -> m.invoke(mgr, tmp));
        }
    }

    @Test
    void cacheShard_flush_write_throws_is_handled() throws Exception {
        Path tmp = Files.createTempDirectory("ci_shard_write_throw");
        int maxKey = 4, maxVal = 4;
        int recordSize = Long.BYTES + Short.BYTES + maxKey + Short.BYTES + maxVal + Long.BYTES;
        Path shardPath = tmp.resolve("s2.dat");
        CacheShard shard = new CacheShard(shardPath, recordSize, 1, maxKey, maxVal);

        // prepare a mapped buffer so flush tries to write; mapped buffer is assignable to MappedByteBuffer
        Field bufF = CacheShard.class.getDeclaredField("buffer");
        bufF.setAccessible(true);
        try (FileChannel real = FileChannel.open(shardPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            java.nio.MappedByteBuffer mapped = real.map(java.nio.channels.FileChannel.MapMode.READ_WRITE, 0, recordSize);
            bufF.set(shard, mapped);
        }

        // mock FileChannel.open to return a channel that throws on write and close
        try (MockedStatic<FileChannel> mfc = mockStatic(FileChannel.class)) {
            FileChannel fake = new FileChannel() {
                @Override public int read(ByteBuffer dst) { return 0; }
                @Override public long read(ByteBuffer[] dsts, int offset, int length) { return 0; }
                @Override public int write(ByteBuffer src) throws IOException { throw new IOException("write fail"); }
                @Override public long write(ByteBuffer[] srcs, int offset, int length) { return 0; }
                @Override public long position() { return 0; }
                @Override public FileChannel position(long newPosition) { return this; }
                @Override public long size() { return 0; }
                @Override public FileChannel truncate(long size) { return this; }
                @Override public void force(boolean metaData) { }
                @Override public long transferTo(long position, long count, java.nio.channels.WritableByteChannel target) { return 0; }
                @Override public long transferFrom(java.nio.channels.ReadableByteChannel src, long position, long count) { return 0; }
                @Override public int read(java.nio.ByteBuffer dst, long position) { return 0; }
                @Override public int write(java.nio.ByteBuffer src, long position) { return 0; }
                @Override
                public java.nio.MappedByteBuffer map(java.nio.channels.FileChannel.MapMode mode, long position, long size) throws IOException {
                    throw new IOException("map not supported");
                }
                @Override public java.nio.channels.FileLock lock(long position, long size, boolean shared) throws IOException { throw new IOException("lock not supported"); }
                @Override public java.nio.channels.FileLock tryLock(long position, long size, boolean shared) throws IOException { throw new IOException("tryLock not supported"); }
                @Override protected void implCloseChannel() throws IOException { throw new IOException("close fail"); }
            };
            mfc.when(() -> FileChannel.open(shardPath, StandardOpenOption.READ, StandardOpenOption.WRITE)).thenReturn(fake);

            // close should catch exceptions
            assertDoesNotThrow(shard::close);
        }
    }

}
