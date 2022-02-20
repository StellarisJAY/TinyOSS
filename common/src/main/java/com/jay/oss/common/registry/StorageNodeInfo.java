package com.jay.oss.common.registry;

import lombok.*;

import java.util.Objects;

/**
 * <p>
 *  存储节点信息
 * </p>
 *
 * @author Jay
 * @date 2022/02/14 10:32
 */
@Builder
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class StorageNodeInfo {
    /**
     * 存储节点地址
     */
    private String url;
    /**
     * 存储节点组
     */
    private String group;

    /**
     * 存储节点在组内的角色
     */
    private String role;
    /**
     * 节点的object事务ID
     */
    private long fxid;

    /**
     * 节点的存储桶事务id
     */
    private long bxid;

    /**
     * 存储节点剩余空间
     */
    private long space;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StorageNodeInfo that = (StorageNodeInfo) o;
        return Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url);
    }
}
