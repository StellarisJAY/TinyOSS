package com.jay.oss.storage.edit;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 *  编辑日志
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
     * 操作类型，添加、删除
     */
    private EditOperation operation;
    /**
     * 操作的内容
     * 如果是添加，content就是元数据的序列化bytes
     * 如果是删除，content是key的bytes
     */
    private byte[] content;
}
