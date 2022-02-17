package com.jay.oss.common.acl;

/**
 * <p>
 *  访问桶模式
 *  读、写
 * </p>
 *
 * @author Jay
 * @date 2022/02/17 11:00
 */
public enum BucketAccessMode {
    /**
     * 读模式
     */
    READ,
    /**
     * 写模式
     */
    WRITE,
    /**
     * 修改权限
     */
    WRITE_ACL
}
