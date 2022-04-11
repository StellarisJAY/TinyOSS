import com.jay.oss.common.bitcask.Chunk;
import com.jay.oss.common.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * <p>
 *  BitCask Chunk读写性能对比 单元测试
 * </p>
 *
 * @author Jay
 * @date 2022/04/11 11:50
 */
@Slf4j
public class TestBitCaskChunk {

    @Test
    public void testChunkWriteMmap() throws IOException {
        long initStart = System.currentTimeMillis();
        Chunk chunk = Chunk.getNewChunk(0);
        log.info("Init time used: {}s", (System.currentTimeMillis() - initStart));

        byte[] key = StringUtil.getBytes("k11");
        byte[] value = StringUtil.getBytes("value");

        long writeStart = System.currentTimeMillis();
        for(int i = 0; i < 10000; i ++){
            chunk.writeMmap(key, value);
        }
        log.info("Write time used: {}ms", (System.currentTimeMillis() - writeStart));
    }

    @Test
    public void testChunkWrite() throws IOException{
        long initStart = System.currentTimeMillis();
        Chunk chunk = Chunk.getNewChunk(1);
        log.info("Init time used: {}ms", (System.currentTimeMillis() - initStart));

        byte[] key = StringUtil.getBytes("key1");
        byte[] value = StringUtil.getBytes("value");

        long writeStart = System.currentTimeMillis();
        for(int i = 0; i < 10000; i ++){
            chunk.write(key, value);
        }
        log.info("Write time used: {}ms", (System.currentTimeMillis() - writeStart));
    }

    @Test
    public void testChunkReadMmap() throws IOException {
        long initStart = System.currentTimeMillis();
        Chunk chunk = Chunk.getNewChunk(0);
        log.info("Init time used: {}ms", (System.currentTimeMillis() - initStart));

        long writeStart = System.currentTimeMillis();
        for(int i = 0; i < 10000; i++){
            byte[] bytes = chunk.readMmap(17 * i);
            Assert.assertEquals(StringUtil.toString(bytes), "value");
        }
        log.info("Write time used: {}ms", (System.currentTimeMillis() - writeStart));
    }

    @Test
    public void testChunkRead() throws IOException {
        long initStart = System.currentTimeMillis();
        Chunk chunk = Chunk.getNewChunk(1);
        log.info("Init time used: {}ms", (System.currentTimeMillis() - initStart));

        long writeStart = System.currentTimeMillis();
        for(int i = 0; i < 10000; i++){
            byte[] bytes = chunk.read(17 * i);
            Assert.assertEquals(StringUtil.toString(bytes), "value");
        }
        log.info("Write time used: {}ms", (System.currentTimeMillis() - writeStart));
    }
}
