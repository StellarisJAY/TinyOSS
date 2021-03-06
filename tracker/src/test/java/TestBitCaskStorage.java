import com.jay.dove.serialize.Serializer;
import com.jay.oss.common.bitcask.BitCaskStorage;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.common.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.units.qual.C;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * <p>
 *  BitCask存储模型单元测试
 * </p>
 *
 * @author Jay
 * @date 2022/04/07 15:43
 */
@Slf4j
public class TestBitCaskStorage {
    ObjectMeta objectMeta = ObjectMeta.builder()
            .objectKey("bucket-124214331123/object1.png")
            .fileName("object.png").createTime(System.currentTimeMillis())
            .size(1024L).versionId("").md5("1a2dafdeeeei2u1233838883333")
            .locations("10.0.0.1:9999;10.0.0.1:9999;10.0.0.1:9999")
            .build();
    /**
     * 测试get和put功能
     * @throws IOException e
     */
    @Test
    public void testGetAndPut() throws IOException {
        BitCaskStorage bitCaskStorage = new BitCaskStorage();

        String val1 = "hello";
        String val2 = "world";

        bitCaskStorage.put("k1", StringUtil.getBytes(val1));
        bitCaskStorage.put("k2", StringUtil.getBytes(val2));

        Assert.assertEquals(val1, StringUtil.toString(bitCaskStorage.get("k1")));
        Assert.assertEquals(val2, StringUtil.toString(bitCaskStorage.get("k2")));
    }

    /**
     * 测试写入性能
     * @throws Exception e
     */
    @Test
    public void testPutSpeed() throws Exception {
        // 初始化
        BitCaskStorage bitCaskStorage = new BitCaskStorage();
        bitCaskStorage.init();
        // 序列化value
        Serializer serializer = new ProtostuffSerializer();
        byte[] value = serializer.serialize(objectMeta, ObjectMeta.class);
        String keyPrefix = "object";

        // 写入100万个KV对
        int total = 1000000;
        long putStart = System.currentTimeMillis();
        for(int i = 0; i < total; i++){
            bitCaskStorage.put(keyPrefix + i + ".png", value);
        }
        long wTime = System.currentTimeMillis() - putStart;
        log.info("Write time used: {}ms, written: {} kv, speed: {} /s", wTime,total, (total * 1000) / wTime);


        // 读取100万个KV对
        long getStart = System.currentTimeMillis();
        for(int i = 0; i < total; i++){
            bitCaskStorage.get(keyPrefix + i + ".png");
        }
        long rTime = System.currentTimeMillis() - getStart;
        log.info("Read time used: {}ms, read: {} kv, speed: {} /s", rTime,total, (total * 1000) / rTime);
    }

    @Test
    public void testWrite(){
        BitCaskStorage bitCaskStorage = new BitCaskStorage();
        for(int i = 0; i < 10; i ++){
            bitCaskStorage.put("k" + i, StringUtil.getBytes("v1"));
        }
    }

    /**
     * 测试初始化时的加载index和compact
     */
    @Test
    public void testInit() throws Exception {
        BitCaskStorage bitCaskStorage = new BitCaskStorage();
        long compactStart = System.currentTimeMillis();
        bitCaskStorage.init();
        log.info("Init time used: {}ms", (System.currentTimeMillis() - compactStart));
    }

    @Test
    public void testConcurrentIO() throws Exception {
        // 初始化
        BitCaskStorage bitCaskStorage = new BitCaskStorage();
        bitCaskStorage.init();
        // 序列化value
        Serializer serializer = new ProtostuffSerializer();
        byte[] value = serializer.serialize(objectMeta, ObjectMeta.class);
        String keyPrefix = "object";

        // 线程数
        int threadCount = 1000;
        // 每个线程write的KV对数量
        int loop = 1000;
        int total = threadCount * loop;

        // 并发写入
        CountDownLatch writeCountDown = new CountDownLatch(threadCount);
        long putStart = System.currentTimeMillis();
        for(int i = 0; i < threadCount; i++){
            int threadNum = i;
            Thread thread = new Thread(() -> {
                for (int j = 0; j < loop; j++) {
                    bitCaskStorage.put(keyPrefix + threadNum + "-" + j, value);
                }
                writeCountDown.countDown();
            });
            thread.start();
        }
        writeCountDown.await();
        long wTime = System.currentTimeMillis() - putStart;
        log.info("Write time used: {}ms, written: {} kv, speed: {} /s", wTime,total, (total * 1000L) / wTime);

        // 并发读取
        CountDownLatch readCountDown = new CountDownLatch(threadCount);
        long readStart = System.currentTimeMillis();
        for(int i = 0; i < threadCount; i++){
            int threadNum = i;
            Thread thread = new Thread(() -> {
                for (int j = 0; j < loop; j++) {
                    bitCaskStorage.put(keyPrefix + threadNum + "-" + j, value);
                }
                readCountDown.countDown();
            });
            thread.start();
        }
        readCountDown.await();
        long rTime = System.currentTimeMillis() - readStart;
        log.info("Read time used: {}ms, read: {} kv, speed: {} /s", rTime,total, (total * 1000L) / rTime);
    }
}
