package com.jay.oss.storage.fs;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 *  对象索引
 *  记录对象在Block中的位置
 * </p>
 *
 * @author Jay
 * @date 2022/04/13 11:05
 */
@Getter
@Setter
@AllArgsConstructor
@ToString
public class ObjectIndex {
    private int blockId;
    private int offset;
    private int size;
    private boolean removed;
}
