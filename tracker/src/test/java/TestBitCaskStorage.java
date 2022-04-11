import com.jay.dove.serialize.Serializer;
import com.jay.oss.common.bitcask.BitCaskStorage;
import com.jay.oss.common.bitcask.Index;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.common.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

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
        BitCaskStorage bitCaskStorage = new BitCaskStorage();
        bitCaskStorage.init();
        Serializer serializer = new ProtostuffSerializer();
        byte[] value = serializer.serialize(objectMeta, ObjectMeta.class);
        String keyPrefix = "object";

        int total = 100000;
        long putStart = System.currentTimeMillis();
        for(int i = 0; i < total; i++){
            bitCaskStorage.put(keyPrefix + i + ".png", value);
        }
        long wTime = System.currentTimeMillis() - putStart;
        log.info("Write time used: {}ms, written: {} kv, speed: {} /s", wTime,total, (total * 1000) / wTime);

        Random random = new Random();
        int i = random.nextInt(total);
        Assert.assertArrayEquals(value, bitCaskStorage.get(keyPrefix + i + ".png"));
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
}
