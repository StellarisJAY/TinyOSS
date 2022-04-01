package com.jay.oss.common.remoting;

import com.jay.dove.transport.HeartBeatTrigger;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.CommandHandler;
import com.jay.dove.transport.protocol.*;

/**
 * <p>
 *  FastOss TCP通信协议
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 14:34
 */
public class FastOssProtocol implements Protocol {
    /**
     * 协议编号
     */
    public static final ProtocolCode PROTOCOL_CODE = ProtocolCode.fromValue((byte)24);
    /**
     * 首部长度
     */
    public static final int HEADER_LENGTH = 21;

    /**
     * decoder
     */
    private final ProtocolDecoder decoder = new FastOssProtocolDecoder();
    /**
     * encoder
     */
    private final ProtocolEncoder encoder = new FastOssProtocolEncoder();


    private final CommandFactory commandFactory = new FastOssCommandFactory();
    /**
     * 上传文件头命令，该命令作用是让StorageNode开启上传流程
     */
    public static final CommandCode UPLOAD_FILE_HEADER = new CommandCode((short)101);
    /**
     * 上传文件分片，该命令的数据部分为文件的一个分片
     */
    public static final CommandCode UPLOAD_FILE_PARTS = new CommandCode((short)102);
    public static final CommandCode RESPONSE_UPLOAD_DONE = new CommandCode((short)103);

    /**
     * 下载完整对象命令，该命令表示当前下载请求要求读取整个对象
     */
    public static final CommandCode DOWNLOAD_FULL = new CommandCode((short)104);
    /**
     * Ranged下载命令，该命令表示当前下载只下载对象的指定范围
     */
    public static final CommandCode DOWNLOAD_RANGED = new CommandCode((short)105);
    public static final CommandCode DOWNLOAD_RESPONSE = new CommandCode((short)106);


    public static final CommandCode DELETE_OBJECT = new CommandCode((short)107);
    public static final CommandCode LOCATE_OBJECT = new CommandCode((short)108);
    public static final CommandCode GET_OBJECT_META = new CommandCode((short)109);
    /**
     * 对象不存在返回
     */
    public static final CommandCode OBJECT_NOT_FOUND = new CommandCode((short)110);

    public static final CommandCode PUT_BUCKET = new CommandCode((short)111);
    public static final CommandCode LIST_BUCKET = new CommandCode((short)112);
    public static final CommandCode CHECK_BUCKET_ACL = new CommandCode((short)113);
    public static final CommandCode BUCKET_PUT_OBJECT = new CommandCode((short)114);
    public static final CommandCode BUCKET_DELETE_OBJECT = new CommandCode((short)115);
    public static final CommandCode GET_SERVICE = new CommandCode((short)116);
    public static final CommandCode UPDATE_BUCKET_ACL = new CommandCode((short)117);

    public static final CommandCode ASYNC_BACKUP = new CommandCode((short)120);
    public static final CommandCode ASYNC_BACKUP_PART = new CommandCode((short)121);

    public static final CommandCode INIT_MULTIPART_UPLOAD = new CommandCode((short)131);
    public static final CommandCode MULTIPART_UPLOAD_PART = new CommandCode((short)132);
    public static final CommandCode LOOKUP_MULTIPART_UPLOAD = new CommandCode((short)133);
    public static final CommandCode MULTIPART_UPLOAD_FINISHED = new CommandCode((short)134);
    public static final CommandCode COMPLETE_MULTIPART_UPLOAD = new CommandCode((short)135);
    public static final CommandCode CANCEL_MULTIPART_UPLOAD = new CommandCode((short)136);

    public static final CommandCode SUCCESS = new CommandCode((short)200);
    public static final CommandCode ERROR = new CommandCode((short)500);
    public static final CommandCode REQUEST_TIMEOUT = new CommandCode((short)502);
    public static final CommandCode ACCESS_DENIED = new CommandCode((short)403);
    public static final CommandCode NOT_FOUND = new CommandCode((short)404);
    public static final CommandCode NO_ENOUGH_STORAGES = new CommandCode((short)600);
    public static final CommandCode DUPLICATE_OBJECT_KEY = new CommandCode((short)700);
    /**
     * 默认命令处理器
     */
    private final CommandHandler commandHandler;
    /**
     * 默认心跳处理器
     */
    private final HeartBeatTrigger heartBeatTrigger;
    public FastOssProtocol(CommandHandler commandHandler) {
        this.commandHandler = commandHandler;
        this.heartBeatTrigger = new FastOssHeartBeatTrigger();
    }

    @Override
    public ProtocolEncoder getEncoder() {
        return encoder;
    }

    @Override
    public ProtocolM2mEncoder getM2mEncoder() {
        return new FastOssM2mEncoder();
    }

    @Override
    public ProtocolDecoder getDecoder() {
        return decoder;
    }

    @Override
    public ProtocolCode getCode() {
        return PROTOCOL_CODE;
    }

    @Override
    public CommandHandler getCommandHandler() {
        return commandHandler;
    }

    @Override
    public HeartBeatTrigger getHeartBeatTrigger() {
        return heartBeatTrigger;
    }

    @Override
    public CommandFactory getCommandFactory() {
        return commandFactory;
    }
}
