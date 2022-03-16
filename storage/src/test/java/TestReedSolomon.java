import com.backblaze.erasure.ReedSolomon;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * <p>
 *  测试ReedSolomon纠删码
 * </p>
 *
 * @author Jay
 * @date 2022/03/16 20:10
 */
public class TestReedSolomon {

    int DATA_SHARD_COUNT = 4;
    int PARITY_SHARD_COUNT = 2;
    int SHARD_COUNT = 6;

    String filePath = "C:\\Users\\76040\\Desktop\\testRS\\multipart.png";
    String outputPath = "C:\\Users\\76040\\Desktop\\testRS\\";


    String decodePath = "C:\\Users\\76040\\Desktop\\testRS\\reconstruct\\";
    String decodeOutput = "C:\\Users\\76040\\Desktop\\testRS\\reconstruct\\decoded";
    @Test
    public void encode() throws IOException {
        File file = new File(filePath);
        int totalSize = (int)file.length();
        FileInputStream inputStream = new FileInputStream(file);
        FileChannel channel = inputStream.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        channel.read(buffer);
        channel.close();
        inputStream.close();
        buffer.rewind();
        // 固定分片大小
        int shardSize = (totalSize + DATA_SHARD_COUNT - 1) / DATA_SHARD_COUNT + 1;
        // 分片数据数组
        byte[][] shards = new byte[SHARD_COUNT][shardSize];

        for (byte[] shard : shards) {
            // buffer剩余大小，需要根据剩余大小来判断是否在数组中填充(byte)0
            int bufferRemaining = buffer.remaining();
            if(shardSize > buffer.remaining()){
                buffer.get(shard, 0, bufferRemaining);
                Arrays.fill(shard, bufferRemaining, shardSize, (byte)0);
            }else{
                buffer.get(shard);
            }
        }

        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARD_COUNT, PARITY_SHARD_COUNT);
        // 生成ReedSolomon纠删码分块
        reedSolomon.encodeParity(shards, 0, shardSize);

        // 写入文件
        for (int i = 0; i < shards.length; i++) {
            File out = new File(outputPath + "output-" + i);
            out.createNewFile();
            FileOutputStream outputStream = new FileOutputStream(out);
            outputStream.write(shards[i]);
            outputStream.close();
        }
    }

    @Test
    public void decode() throws IOException{
        int totalSize = 4057287;
        int shardSize = (totalSize + DATA_SHARD_COUNT - 1) / DATA_SHARD_COUNT + 1;
        // 分片数据
        byte[][] shards = new byte[SHARD_COUNT][shardSize];
        // 分片缺失数组
        boolean[] shardPresent = new boolean[SHARD_COUNT];
        // 判断哪些分片缺失
        for (int i = 0; i < SHARD_COUNT; i++) {
            File file = new File(decodePath + "output-" + i);
            if(file.exists()){
               shardPresent[i] = true;
                FileInputStream inputStream = new FileInputStream(file);
                inputStream.read(shards[i]);
                inputStream.close();
            }
        }
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARD_COUNT, PARITY_SHARD_COUNT);
        // 从剩余的分片计算出缺失分片数据
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        // 写入文件
        File file = new File(decodeOutput);
        file.createNewFile();
        FileOutputStream outputStream = new FileOutputStream(file);
        for (int i = 0; i < DATA_SHARD_COUNT; i++) {
            outputStream.write(shards[i]);
        }
        outputStream.close();
    }
}
