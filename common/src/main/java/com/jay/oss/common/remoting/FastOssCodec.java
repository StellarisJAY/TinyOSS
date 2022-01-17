package com.jay.oss.common.remoting;

import com.jay.dove.transport.codec.Codec;
import com.jay.dove.transport.codec.ProtocolCodeBasedDecoder;
import com.jay.dove.transport.codec.ProtocolCodeBasedEncoder;
import io.netty.channel.ChannelHandler;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 14:35
 */
public class FastOssCodec implements Codec {
    @Override
    public ChannelHandler newDecoder() {
        return new ProtocolCodeBasedDecoder();
    }

    @Override
    public ChannelHandler newEncoder() {
        return new ProtocolCodeBasedEncoder(FastOssProtocol.PROTOCOL_CODE);
    }
}
