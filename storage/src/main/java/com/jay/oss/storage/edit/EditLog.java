package com.jay.oss.storage.edit;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/20 11:40
 */
@AllArgsConstructor
@Getter
@Setter
@ToString
public class EditLog {
    /**
     * 事务ID
     */
    private long xid;
    /**
     * 事务类型，1 object 2 bucket
     */
    private byte type;
    /**
     * 操作类型
     */
    private EditOperation operation;
    /**
     * 操作的key
     */
    private String key;
}
