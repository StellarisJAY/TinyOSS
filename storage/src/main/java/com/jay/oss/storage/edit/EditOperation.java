package com.jay.oss.storage.edit;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/20 12:23
 */
public enum EditOperation {
    /**
     * 删除操作
     */
    DELETE((byte)0),
    ;
    private byte code;

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
