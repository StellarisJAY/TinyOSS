package com.jay.oss.common.remoting;

import com.jay.dove.transport.connection.AbstractConnectionFactory;
import com.jay.dove.transport.connection.ConnectEventHandler;

/**
 * <p>
 *  FastOss 连接工厂
 * </p>
 *
 * @author Jay
 * @date 2022/01/27 10:03
 */
public class TinyOssConnectionFactory extends AbstractConnectionFactory {
    public TinyOssConnectionFactory() {
        super(new TinyOssCodec(), TinyOssProtocol.PROTOCOL_CODE, new ConnectEventHandler());
    }
}
