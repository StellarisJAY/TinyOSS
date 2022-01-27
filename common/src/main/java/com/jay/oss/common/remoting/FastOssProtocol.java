package com.jay.oss.common.remoting;

import com.jay.dove.transport.HeartBeatTrigger;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandHandler;
import com.jay.dove.transport.protocol.Protocol;
import com.jay.dove.transport.protocol.ProtocolCode;
import com.jay.dove.transport.protocol.ProtocolDecoder;
import com.jay.dove.transport.protocol.ProtocolEncoder;

/**
 * <p>
 *  FastOss inet remoting Protocol
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 14:34
 */
public class FastOssProtocol implements Protocol {

    public static final ProtocolCode PROTOCOL_CODE = ProtocolCode.fromValue((byte)24);
    public static final int HEADER_LENGTH = 21;

    private final ProtocolDecoder decoder = new FastOssProtocolDecoder();
    private final ProtocolEncoder encoder = new FastOssProtocolEncoder();

    public static final CommandCode UPLOAD_FILE_HEADER = new CommandCode((short)101);
    public static final CommandCode UPLOAD_FILE_PARTS = new CommandCode((short)102);
    public static final CommandCode RESPONSE_UPLOAD_DONE = new CommandCode((short)103);


    public static final CommandCode SUCCESS = new CommandCode((short)200);
    public static final CommandCode ERROR = new CommandCode((short)500);
    public static final CommandCode REQUEST_TIMEOUT = new CommandCode((short)502);

    private CommandHandler commandHandler;

    public FastOssProtocol(CommandHandler commandHandler) {
        this.commandHandler = commandHandler;
    }

    @Override
    public ProtocolEncoder getEncoder() {
        return encoder;
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

    public void setCommandHandler(CommandHandler commandHandler){
        this.commandHandler = commandHandler;
    }

    @Override
    public HeartBeatTrigger getHeartBeatTrigger() {
        return null;
    }
}
