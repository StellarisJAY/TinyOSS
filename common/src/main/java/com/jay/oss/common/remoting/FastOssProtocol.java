package com.jay.oss.common.remoting;

import com.jay.dove.transport.HeartBeatTrigger;
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
        return null;
    }

    @Override
    public HeartBeatTrigger getHeartBeatTrigger() {
        return null;
    }
}
