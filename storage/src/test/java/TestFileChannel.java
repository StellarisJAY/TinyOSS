import com.jay.oss.common.config.OssConfigs;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/04/15 11:30
 */
@Slf4j
public class TestFileChannel {

    @Test
    public void testWrite10KB() throws IOException {
        int bytes = 10 * 1024;
        ByteBuffer kb10 = ByteBuffer.allocate(bytes);

        File file = new File(OssConfigs.dataPath() + File.separator + "channel_"+bytes);
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        FileChannel channel = randomAccessFile.getChannel();

        int total = 1024 * 1024 * 1000;
        int loop = total / bytes;
        long start = System.currentTimeMillis();
        for(int i = 0; i < loop; i++){
            int offset = i * bytes;
            channel.write(kb10, offset);
            kb10.rewind();
        }
        log.info("Time used: {}ms", System.currentTimeMillis() - start);
    }

    @Test
    public void testWrite16KB() throws IOException {
        int bytes = 64 * 1024;
        File src = new File("D:/dest250.log");
        RandomAccessFile rf = new RandomAccessFile(src, "r");
        FileChannel srcChannel = rf.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate((int) srcChannel.size());
        srcChannel.read(buffer);
        buffer.rewind();
        log.info("size: {}", buffer.remaining());

        File file = new File(OssConfigs.dataPath() + File.separator + "channel_" + bytes);
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        FileChannel channel = randomAccessFile.getChannel();

        int total = buffer.remaining();
        int loop = (int)Math.ceil((double) total / bytes);
        long start = System.currentTimeMillis();
        for(int i = 0; i < loop; i++){
            int offset = i * bytes;
            ByteBuffer slice = buffer.slice();
            slice.position(offset);
            slice.limit(i == loop - 1 ? buffer.limit() : offset + bytes);
            channel.write(slice, offset);
        }
        log.info("loop: {} Time used: {}ms",loop, System.currentTimeMillis() - start);
    }
    @Test
    public void testWriteFull() throws IOException {
        File src = new File("D:/dest250.log");
        RandomAccessFile rf = new RandomAccessFile(src, "r");
        FileChannel srcChannel = rf.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate((int) srcChannel.size());
        srcChannel.read(buffer);
        buffer.rewind();
        log.info("size: {}", buffer.remaining());

        File file = new File(OssConfigs.dataPath() + File.separator + "channel_" + System.currentTimeMillis());
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        FileChannel channel = randomAccessFile.getChannel();
        long start = System.currentTimeMillis();
        channel.write(buffer);
        log.info("Time used: {}ms", System.currentTimeMillis() - start);
    }
}
