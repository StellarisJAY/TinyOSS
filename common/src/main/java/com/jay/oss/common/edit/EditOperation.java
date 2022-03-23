package com.jay.oss.common.edit;

/**
 * <p>
 *  编辑日志操作类型
 * </p>
 *
 * @author Jay
 * @date 2022/02/20 12:23
 */
public enum EditOperation {
    /**
     * 添加meta，包括object和bucket
     */
    ADD((byte)1),
    /**
     * 删除操作，包括storage删除元数据、tracker删除存储桶
     */
    DELETE((byte)2),
    /**
     * 向存储桶放入对象元数据
     */
    BUCKET_PUT_OBJECT((byte)3),
    /**
     * 删除桶内对象操作
     */
    BUCKET_DELETE_OBJECT((byte)4),
    /**
     * 分片上传任务
     */
    MULTIPART_UPLOAD((byte)5),
    /**
     * 标记删除
     */
    MARK_DELETE((byte)6)
    ;
    private final byte code;

    EditOperation(byte code) {
        this.code = code;
    }

    public byte value(){
        return code;
    }

    public static EditOperation get(byte code){
        for (EditOperation value : EditOperation.values()) {
            if(value.code == code){
                return value;
            }
        }
        return null;
    }
}
