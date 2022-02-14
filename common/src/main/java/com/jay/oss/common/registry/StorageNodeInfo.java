package com.jay.oss.common.registry;

import lombok.*;

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
     * 节点的事务ID
     */
    private long txId;

    /**
     * 存储节点剩余空间
     */
    private long space;
}
