package com.jay.oss.common.registry;

import lombok.*;

import java.io.Serializable;
import java.util.List;
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
public class StorageNodeInfo implements Serializable {
    /**
     * 存储节点地址
     */
    private String url;
    /**
     * 存储节点剩余空间
     */
    private long space;

    /**
     * 节点已经使用了的空间
     */
    private long usedSpace;

    private boolean available = true;

    /**
     * 两次心跳之间的IO次数
     * 该属性用来判断Storage服务器的繁忙程度
     */
    private long ioRate;

    /**
     * 可用的总内存大小
     * 该属性用来判断Storage在集群中的权重
     */
    private long memoryTotal;

    private List<Long> storedObjects;


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StorageNodeInfo that = (StorageNodeInfo) o;
        return Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url);
    }
}
