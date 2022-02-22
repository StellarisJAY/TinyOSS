package com.jay.oss.tracker.track;

import com.jay.oss.common.registry.StorageNodeInfo;

import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 12:53
 */
public class RandomStorageSelector extends AbstractStorageSelector{
    @Override
    public List<StorageNodeInfo> doSelect(List<StorageNodeInfo> nodes, long size, int replica) {
        return nodes.subList(0, 1);
    }
}
