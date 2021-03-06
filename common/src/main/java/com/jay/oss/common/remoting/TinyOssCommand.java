package com.jay.oss.common.remoting;

import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.DefaultFileRegion;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 *  Fast-OSS remoting command.
 *  +---------+----------+------+--------+-----------+----------+------------+
 *  |  proto  |  length  |  id  |  code  |  timeout  |  serial  |  compress  |
 *  +---------+----------+------+--------+-----------+----------+------------+
 *  |                                                                        |
 *  |                            content                                     |
 *  |                                                                        |
 *  +------------------------------------------------------------------------+
 *
 *  proto: protocol code,  1 byte, value 24.
 *  length: Header + content, 4 bytes.
 *  id: command id, 4 bytes.
 *  code: command code, 2 bytes.
 *  timeout: timeout mills, used by server-side fail-fast, 8 bytes long.
 *  serial: serializer code, 1 byte.
 *  compress: compressor code, 1 byte, -1 means un-compressed.
 *
 *  Header Length: 21
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 14:34
 */
@Builder
@Getter
@Setter
@ToString
public class TinyOssCommand implements RemotingCommand {

    private int id;
    private int length;
    private CommandCode commandCode;
    private long timeout;
    private byte serializer;
    private byte compressor;

    private byte[] content;
    private ByteBuf data;
    private DefaultFileRegion fileRegion;

    @Override
    public int getId() {
        return id;
    }

    @Override
    public void setId(int i) {
        this.id = i;
    }

    @Override
    public byte getSerializer() {
        return serializer;
    }

    @Override
    public CommandCode getCommandCode() {
        return commandCode;
    }

    @Override
    public long getTimeoutMillis() {
        return timeout;
    }

    @Override
    public void setTimeoutMillis(long l) {
        this.timeout = l;
    }

    @Override
    public byte[] getContent() {
        return content;
    }
}
