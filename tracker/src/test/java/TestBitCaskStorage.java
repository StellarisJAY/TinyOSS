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
     * 测试compact功能和写入性能
     * @throws Exception e
     */
    @Test
    public void testCompact() throws Exception {
        BitCaskStorage bitCaskStorage = new BitCaskStorage();
        bitCaskStorage.init();
        Serializer serializer = new ProtostuffSerializer();
        ObjectMeta objectMeta = ObjectMeta.builder().objectKey("bucket-124214331123/object1.png").fileName("object.png").createTime(System.currentTimeMillis())
                .size(1024L).versionId("").md5("1a2dafdeeeei2u1233838883333").locations("10.0.0.1:9999;10.0.0.1:9999;10.0.0.1:9999")
                .build();
        byte[] value = serializer.serialize(objectMeta, ObjectMeta.class);
        log.info("Data Size: {}", value.length);

        /*
            Start Test Here
         */
        int total = 1000;
        long putStart = System.currentTimeMillis();
        for(int i = 0; i < total; i++){
            bitCaskStorage.put("object" + i + ".png", value);
        }
        long wTime = System.currentTimeMillis() - putStart;
        log.info("Write time used: {}ms, speed: {} /s", wTime, (total * 1000) / wTime);
        long compactStart = System.currentTimeMillis();
        bitCaskStorage.compact();
        log.info("Compact time used: {}ms", (System.currentTimeMillis() - compactStart));
    }

    /**
     * 测试初始化时加载kv索引
     * @throws Exception e
     */
    @Test
    public void testParseChunks() throws Exception {
        BitCaskStorage bitCaskStorage = new BitCaskStorage();
        bitCaskStorage.init();

        Index index0 = bitCaskStorage.getIndex("object0.png");
        Index index1 = bitCaskStorage.getIndex("object1.png");
        Index index2 = bitCaskStorage.getIndex("object2.png");

        Assert.assertNotNull(index0);
        Assert.assertNotNull(index1);
        Assert.assertNotNull(index2);
        Assert.assertEquals(0, index0.getOffset());
        Assert.assertEquals(148, index1.getOffset());
        Assert.assertEquals(296, index2.getOffset());
    }
}
