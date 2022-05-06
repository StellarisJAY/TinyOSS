package com.jay.oss.common.kv;

/**
 * <p>
 *  EditLog Operation
 * </p>
 *
 * @author Jay
 * @date 2022/05/06 14:04
 */
public enum EditOperation {
    /**
     * PUT opt
     */
    PUT((byte)1),
    /**
     * DELETE opt
     */
    DELETE((byte)2);

    final byte value;
    EditOperation(byte value){
        this.value = value;
    }

    public byte value(){
        return value;
    }

    public static EditOperation getOperation(byte value){
        switch(value){
            case 1: return PUT;
            case 2: return DELETE;
            default: return null;
        }
    }
}
