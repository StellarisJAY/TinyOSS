package com.jay.oss.common.remoting;

import com.jay.dove.transport.codec.Codec;
import com.jay.dove.transport.codec.ProtocolBasedM2mEncoder;
import com.jay.dove.transport.codec.ProtocolCodeBasedDecoder;
import com.jay.dove.transport.codec.ProtocolCodeBasedEncoder;
import io.netty.channel.ChannelHandler;

/**
 * <p>
 *  FastOSS Codec
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
        // 使用M2M encoder，获取对FileRegion零拷贝传输支持
        return new ProtocolBasedM2mEncoder(FastOssProtocol.PROTOCOL_CODE);
    }
}
