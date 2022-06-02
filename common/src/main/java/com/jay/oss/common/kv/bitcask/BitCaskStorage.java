package com.jay.oss.common.kv.bitcask;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.kv.KvStorage;
import com.jay.oss.common.util.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * <p>
 *  BitCask存储引擎
 *
 *  存储模型：
 *  | keyLen | valLen | key | compressed val |
 *  空间使用计算(假设Key=128 B, 压缩后的value=384 B)：
 *  记录：8 bytes + (key+val) = 8 + 128 + 384 = 520 bytes
 *  索引：key + 8 bytes = 128 + 8 = 136 bytes
 *  单个KV对占用磁盘空间：记录+索引 = 520 + 136 = 656 bytes
 *
 *  假设一共100W个KV对
 *  内存占用 = 索引 * 100W = 136 * 100W = 132812.5 KB = 129.69 MB
 *  磁盘占用 = 记录 * 100W = 520 * 100w = 507812.5 KB = 495.91 MB
 *
 *  假设80%占用量，得出100W数量级下单机内存和磁盘使用情况：
 *  内存 = 130 / 0.8 = 162 MB
 *  磁盘 = 496 / 0.8 = 620 MB
 * </p>
 *
 * @author Jay
 * @date 2022/03/04 10:55
 */
@Slf4j
public class BitCaskStorage implements KvStorage {
    /**
     * Index Hash表
     */
    private final ConcurrentHashMap<String, Index> indexCache = new ConcurrentHashMap<>();

    /**
     * chunk列表，列表下标和chunkId对应
     */
    private final List<Chunk> chunks = new ArrayList<>();

    /**
     * 当前活跃chunk
     */
    private Chunk activeChunk = null;

    /**
     * 删除标记，把一个key的value设置为该标记来表示key被删除了
     * 该标记的值是ASCII字符Del，编号127
     */
    private static final byte[] DELETE_TAG = new byte[]{(byte)127};

    /**
     * writeLock
     */
    private final Object activeChunkLock = new Object();

    /**
     * 压缩old chunk的读写锁
     */
    private final ReentrantReadWriteLock compactLock = new ReentrantReadWriteLock();

    /**
     * chunk文件自增ID
     */
    private final AtomicInteger chunkIdProvider = new AtomicInteger(0);

    public static final String CHUNK_DIRECTORY = File.separator + "chunks";

    /**
     * 初始化BitCask存储模型
     * 首先会加载目录下的chunk文件，然后读取Hint日志来加载索引信息。
     * @throws Exception e
     */
    @Override
    public void init() throws Exception {
        String path = OssConfigs.dataPath() + CHUNK_DIRECTORY;
        File directory = new File(path);
        // 加载文件目录下的属于该存储的chunk文件
        File[] files = directory.listFiles((dir, fileName) -> fileName.startsWith("chunk_"));
        if(files != null){
            for(File chunkFile : files){
                Chunk chunk = Chunk.getChunkInstance(chunkFile);
                if(chunk != null){
                    chunks.add(chunk);
                }
            }
            chunks.sort(Comparator.comparingInt(Chunk::getChunkId));
        }
        log.info("Loaded chunk files: {}", chunks);
        // 加载索引
        loadIndex();
        // 压缩chunks
        compact();
        if(!chunks.isEmpty()){
            chunkIdProvider.set(1);
        }
        log.info("BitCask Storage loaded {} chunks", chunks.size());
    }

    /**
     * 保证activeChunk可写入
     * @throws IOException e
     */
    private void ensureActiveChunk(String key, byte[] value) throws IOException {
        if(this.activeChunk == null || activeChunk.isNotWritable(8 + key.length() + value.length)){
            // 创建新的active chunk
            this.activeChunk = Chunk.getNewChunk(chunkIdProvider.getAndIncrement());
            if(chunks.size() >= activeChunk.getChunkId()){
                chunks.add(activeChunk);
            }else{
                chunks.set(activeChunk.getChunkId(), activeChunk);
            }
        }
    }

    /**
     * get value
     * @param key key
     * @return byte[]
     */
    @Override
    public byte[] get(String key) {
        try{
            compactLock.readLock().lock();
            Index index = indexCache.get(key);
            Chunk chunk;
            if(index  != null && !index.isRemoved() && index.getChunkId() < chunks.size() && (chunk = chunks.get(index.getChunkId())) != null){
                return chunk.readMmap(index.getOffset());
            }
            return null;
        } finally {
            compactLock.readLock().unlock();
        }

    }

    /**
     * put value
     * @param key key
     * @param value value byte[]
     */
    @Override
    public boolean putIfAbsent(String key, byte[] value)  {
        if(!containsKey(key)){
            synchronized (activeChunkLock){
                try{
                    if(!containsKey(key)){
                        ensureActiveChunk(key, value);
                        byte[] keyBytes = StringUtil.getBytes(key);
                        int offset = activeChunk.writeMmap(keyBytes, value);
                        Index index = new Index(activeChunk.getChunkId(), offset, false);
                        indexCache.put(key, index);
                        return true;
                    }
                    return false;
                }catch (IOException e){
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * 更新值
     * @param key key
     * @param value value
     */
    @Override
    public boolean put(String key, byte[] value) {
        synchronized (activeChunkLock){
            try{
                byte[] keyBytes = key.getBytes(OssConfigs.DEFAULT_CHARSET);
                ensureActiveChunk(key, value);
                int offset = activeChunk.writeMmap(keyBytes, value);
                Index index = new Index(activeChunk.getChunkId(), offset, false);
                indexCache.put(key, index);
                return true;
            }catch (IOException e){
                log.warn("Put kv failed, key: {} ",key, e);
                return false;
            }
        }
    }

    /**
     * delete key
     * @param key key
     * @return boolean
     */
    @Override
    public boolean delete(String key){
        if(indexCache.containsKey(key)){
            synchronized (activeChunkLock){
                try{
                    Index index = indexCache.get(key);
                    if(index != null){
                        index.setRemoved(true);
                        ensureActiveChunk(key, DELETE_TAG);
                        activeChunk.writeMmap(StringUtil.getBytes(key), DELETE_TAG);
                        return true;
                    }
                }catch (IOException e){
                    log.warn("BitCask Delete failed, key: {}", key, e);
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * 压缩、合并chunk文件
     * 1、读取所有索引，把没被删除的数据重新写到新的chunk中。
     * 2、修改index的offset和chunkId
     * 3、生成新的HintFile来记录索引
     */
    public void compact(){
        try{
            // 加锁，避免有其他线程读取
            compactLock.writeLock().lock();
            if(indexCache.isEmpty()){
                return;
            }
            // 只有在有其他非活跃Chunk的时候才进行compact，不对activeChunk压缩
            if(activeChunk == null || activeChunk.getChunkId() > 0){
                // 在compact前删除旧的Hint文件，避免compact后没有成功写入Hint文件而导致Hint和实际位置不一致的情况
                removeOldHintFile();
                Chunk mergedChunk = Chunk.getMergeChunkInstance();
                // 对index进行排序，可以使合并后的chunk文件中的键值对有序
                List<Map.Entry<String, Index>> entries = indexCache.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
                for (Map.Entry<String, Index> entry : entries) {
                    String key = entry.getKey();
                    Index index = entry.getValue();
                    // key没有被删除，且不是在activeChunk中
                    if(!index.isRemoved()){
                        if((activeChunk == null || index.getChunkId() != activeChunk.getChunkId())){
                            byte[] keyBytes = StringUtil.getBytes(key);
                            Chunk chunk = chunks.get(index.getChunkId());
                            if(chunk != null){
                                byte[] value = chunk.readMmap(index.getOffset());
                                if(value == null){
                                    break;
                                }
                                // 写入新chunk并更新index
                                int offset = mergedChunk.write(keyBytes, value);
                                index.setChunkId(0);
                                index.setOffset(offset);
                            }
                        }
                    }
                }
                mergedChunk.closeChannel();
                // 生成HintFile
                generateHintFile();
                // 删除合并后的旧chunks，并重命名合并块为chunk0
                deleteOldChunks();
            }
        }catch (IOException e){
            log.warn("Compact BitCask chunk failed ", e);
        }
        finally {
            compactLock.writeLock().unlock();
        }
    }

    /**
     * 删除旧的Hint文件
     */
    private void removeOldHintFile(){
        String path = OssConfigs.dataPath() + File.separator + "hint.log";
        File file = new File(path);
        file.deleteOnExit();
    }

    /**
     * 生成Hint 文件
     */
    private void generateHintFile() throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        for (Map.Entry<String, Index> entry : indexCache.entrySet()) {
            // key没有被删除
            if(!entry.getValue().isRemoved()){
                // key不是在activeChunk中
                if(activeChunk == null || entry.getValue().getChunkId() != activeChunk.getChunkId()){
                    byte[] keyBytes = StringUtil.getBytes(entry.getKey());
                    Index index = entry.getValue();
                    buffer.writeInt(keyBytes.length);
                    buffer.writeBytes(keyBytes);
                    buffer.writeInt(index.getChunkId());
                    buffer.writeInt(index.getOffset());
                }
            }
        }
        String path = OssConfigs.dataPath() + File.separator + "hint.log";
        File file = new File(path);
        if(!file.exists() && !file.createNewFile()){
            throw new RuntimeException("Generate Hint File failed");
        }
        // 写入hint文件
        try(FileOutputStream outputStream = new FileOutputStream(file);
            FileChannel channel = outputStream.getChannel()){
            buffer.readBytes(channel, channel.size(), buffer.readableBytes());
        }catch (IOException e){
            log.warn("Generate Hint File Failed ", e);
        }
    }

    /**
     * 删除除了activeChunk以外的已经完成合并的chunks
     */
    private void deleteOldChunks() throws IOException {
        /*
            删除所有合并后的chunk
         */
        for (Chunk chunk : chunks) {
            if(chunk != activeChunk){
                chunk.closeChannel();
                String path = OssConfigs.dataPath() + CHUNK_DIRECTORY + File.separator + "chunk_" + chunk.getChunkId();
                File chunkFile = new File(path);
                if(chunkFile.exists() && chunkFile.delete()){
                    log.debug("Delete Old Chunk success, chunk: {}", path);
                }else if(!chunkFile.exists()){
                    log.warn("Chunk: {} not exists", path);
                }
            }
        }
        /*
            重命名merged_chunks为chunk0
         */
        File merged = new File(OssConfigs.dataPath() + CHUNK_DIRECTORY + File.separator + "merged_chunks");
        File chunk0 = new File(OssConfigs.dataPath() + CHUNK_DIRECTORY + File.separator + "chunk_0");
        if(merged.exists() && !chunk0.exists() && merged.renameTo(chunk0)){
            Chunk chunk = Chunk.getNewChunk(0);
            chunks.set(0, chunk);
        }else if(!merged.exists()){
            throw new RuntimeException("Merged chunks file doesn't exist");
        }else if(chunk0.exists()){
            throw new RuntimeException("Chunk_0 file already exists");
        }else{
            throw new RuntimeException("Rename merged chunks failed");
        }
    }

    /**
     * 启动时读取Hint文件和chunk文件来加载key的索引信息
     */
    public void loadIndex() {
        String path = OssConfigs.dataPath() + File.separator + "hint.log";
        File hintFile = new File(path);
        // Hint文件存在，扫描Hint中的索引
        if(hintFile.exists()){
            parseHintFile(hintFile);
            indexCache.putAll(chunks.get(chunks.size() - 1).fullScanChunk());
        }else{
            parseAllChunks();
        }
    }

    /**
     * 加载并解析Hint文件
     * 该情况下只解析Hint文件，不解析chunk0
     * @param file {@link File} Hint文件
     */
    private void parseHintFile(File file){
        try(FileInputStream inputStream = new FileInputStream(file);
            FileChannel channel = inputStream.getChannel()){
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeBytes(channel, 0, (int)channel.size());
            while(buffer.isReadable()){
                int keyLen = buffer.readInt();
                byte[] keyBytes = new byte[keyLen];
                buffer.readBytes(keyBytes);
                int chunkId = buffer.readInt();
                int offset = buffer.readInt();
                Index index = new Index(chunkId, offset, false);
                indexCache.put(StringUtil.toString(keyBytes), index);
            }
        }catch (IOException e){
            log.warn("Parse Hint File Failed ", e);
        }
    }

    /**
     * 解析所有的chunk文件
     */
    private void parseAllChunks() {
        for (Chunk chunk : chunks) {
            indexCache.putAll(chunk.fullScanChunk());
        }
    }

    @Override
    public List<String> keys(){
        return new ArrayList<>(indexCache.keySet());
    }

    @Override
    public boolean containsKey(String key) {
        Index index = indexCache.get(key);
        return index != null && !index.isRemoved();
    }
}
