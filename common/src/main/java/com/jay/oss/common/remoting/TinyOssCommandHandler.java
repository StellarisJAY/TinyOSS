package com.jay.oss.common.remoting;

import com.jay.dove.transport.command.AbstractCommandHandler;
import com.jay.dove.transport.command.CommandFactory;

import java.util.concurrent.ExecutorService;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 14:40
 */
public class TinyOssCommandHandler extends AbstractCommandHandler {
    public TinyOssCommandHandler(CommandFactory commandFactory, ExecutorService executor) {
        super(commandFactory);
        this.registerDefaultExecutor(executor);
    }
}
