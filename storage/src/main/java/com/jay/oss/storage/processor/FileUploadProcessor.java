package com.jay.oss.storage.processor;

import com.jay.dove.config.DoveConfigs;
import com.jay.dove.serialize.Serializer;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.entity.FilePart;
import com.jay.oss.common.entity.UploadRequest;
import com.jay.oss.storage.fs.Chunk;
import com.jay.oss.storage.fs.ChunkManager;
import com.jay.oss.storage.fs.FileReceiver;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 *  文件上传处理器
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 14:45
 */
@Slf4j
public class FileUploadProcessor extends AbstractProcessor {
    /**
     * chunk管理器
     */
    private final ChunkManager chunkManager;
    /**
     * 文件接收器map。
     * key：文件key；value：Receiver
     */
    private final ConcurrentHashMap<String, FileReceiver> fileReceivers = new ConcurrentHashMap<>(256);

    /**
     * 元数据管理器
     */
    private final MetaManager metaManager;
    private final CommandFactory commandFactory;
    private final EditLogManager editLogManager;
    public FileUploadProcessor(ChunkManager chunkManager, MetaManager metaManager, EditLogManager editLogManager, CommandFactory commandFactory) {
        this.chunkManager = chunkManager;
        this.metaManager = metaManager;
        this.editLogManager = editLogManager;
        this.commandFactory = commandFactory;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            CommandCode commandCode = command.getCommandCode();
            // 判断CommandCode
            if(FastOssProtocol.UPLOAD_FILE_HEADER.equals(commandCode)){
                processUploadRequest(channelHandlerContext, command);
            }
            else if(FastOssProtocol.UPLOAD_FILE_PARTS.equals(commandCode)){
                processUploadParts(channelHandlerContext, command);
            }
        }
    }

    /**
     * 处理上传请求
     * @param context context
     * @param command {@link FastOssCommand}
     */
    private void processUploadRequest(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();

        /*
            解压 + 反序列化
         */
        Serializer serializer = SerializerManager.getSerializer(command.getSerializer());
        UploadRequest request = serializer.deserialize(content, UploadRequest.class);
        AtomicBoolean duplicateKey = new AtomicBoolean(true);
        /*
            computeIfAbsent 保证同一个key的meta只保存一次
         */
        metaManager.computeIfAbsent(request.getKey(), (k)->{
            // 获取chunk文件
            Chunk chunk = chunkManager.getChunkBySize(request.getSize());
            // 计算该文件的offset
            int offset = chunk.getAndAddSize((int)request.getSize());
            // 创建元数据
            FileMetaWithChunkInfo meta = FileMetaWithChunkInfo.builder()
                    .chunkId(chunk.getId()).removed(false)
                    .offset(offset).size(request.getSize())
                    .key(request.getKey()).filename(request.getFilename())
                    .createTime(System.currentTimeMillis()).build();
            chunk.addObjectMeta(meta);
            // 创建文件接收器
            FileReceiver receiver = FileReceiver.createFileReceiver(chunk, request.getParts(), offset,  chunkManager);
            // 保存文件接收器
            fileReceivers.putIfAbsent(request.getKey(), receiver);
            // 追加edit日志
            appendEditLog(meta);
            duplicateKey.set(false);
            return meta;
        });
        // 没能够成功进行computeIfAbsent的重复的key
        if(duplicateKey.get() && fileReceivers.get(request.getKey()) == null){
            // 发送重复回复报文
            RemotingCommand response = commandFactory.createResponse(command.getId(), request.getKey().getBytes(StandardCharsets.UTF_8), FastOssProtocol.ERROR);
            sendResponse(context, response);
        } else{
            RemotingCommand response = commandFactory.createResponse(command.getId(), request.getKey().getBytes(StandardCharsets.UTF_8), FastOssProtocol.SUCCESS);
            sendResponse(context, response);
        }
    }

    /**
     * 处理上传分片
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void processUploadParts(ChannelHandlerContext context, FastOssCommand command){
        ByteBuf data = command.getData();
        // 读取key长度
        int keyLength = data.readInt();
        // 读取key，并转为String
        byte[] keyBytes = new byte[keyLength];
        data.readBytes(keyBytes);
        String key = new String(keyBytes, DoveConfigs.DEFAULT_CHARSET);
        // 读取分片编号
        int partNum = data.readInt();
        FilePart filePart = FilePart.builder()
                .partNum(partNum)
                .data(data)
                .key(key)
                .build();
        // 通过key找到接收器
        FileReceiver fileReceiver = fileReceivers.get(key);
        RemotingCommand response = null;
        // 写入分片
        try{
            if(fileReceiver.receivePart(filePart)){
                // 已收到最后一个key，删除receiver
                fileReceivers.remove(key);
                response = commandFactory.createResponse(command.getId(), filePart.getKey(), FastOssProtocol.RESPONSE_UPLOAD_DONE);
            }else{
                // 收到部分分片
                response = commandFactory.createResponse(command.getId(), filePart.getKey() + ":" + partNum, FastOssProtocol.SUCCESS);
            }
        }catch (Exception e){
            // 分片接收错误，发送重传请求
            log.warn("receive part failed, part num: {}", partNum);
            log.error("error: ", e);
            response = commandFactory.createResponse(command.getId(), filePart.getKey() + ":" + partNum, FastOssProtocol.ERROR);
        }finally{
            /*
                释放data，避免堆外内存OOM
                只release一个refCnt
             */
            data.release();
            context.channel().writeAndFlush(response);
        }

    }

    private void appendEditLog(FileMetaWithChunkInfo meta){
        byte[] serialized = SerializeUtil.serialize(meta, FileMetaWithChunkInfo.class);
        // 生成编辑日志
        EditLog editLog = new EditLog(EditOperation.ADD, serialized);
        editLogManager.append(editLog);
    }
}
