package com.jay.oss.common.acl;

/**
 * <p>
 *  Access Control
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 10:23
 */
public enum Acl {
    /**
     * 私有
     */
    PRIVATE("private", (byte)1),
    /**
     * 公共读
     */
    PUBLIC_READ("pub_read", (byte)2),
    /**
     * 公共写
     */
    PUBLIC_WRITE("pub_write", (byte)3);

    public final String name;
    public final byte code;

    Acl(String name, byte code) {
        this.name = name;
        this.code = code;
    }

    public static Acl getAcl(String acl){
        return Acl.valueOf(acl.toUpperCase());
    }
}
