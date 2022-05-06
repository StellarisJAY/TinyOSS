package com.jay.oss.common.kv;

import lombok.Getter;
import lombok.Setter;

/**
 * <p>
 *  主从同步日志
 * </p>
 *
 * @author Jay
 * @date 2022/05/06 11:01
 */
@Getter
@Setter
public class EditLog {
    /**
     * 事务ID，通过自增ID来标记每一条操作记录
     */
    private long txId;
    private EditOperation operation;
    private String key;
    private byte[] value;

    public EditLog(EditOperation operation, String key, byte[] value) {
        this.operation = operation;
        this.key = key;
        this.value = value;
    }

    public EditLog(EditOperation operation, String key) {
        this.operation = operation;
        this.key = key;
        this.value = null;
    }
}
