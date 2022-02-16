package com.jay.oss.common.util;

import com.jay.dove.serialize.Serializer;
import com.jay.dove.serialize.SerializerManager;
import com.jay.oss.common.config.OssConfigs;

/**
 * <p>
 *  序列化工具
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 11:30
 */
public class SerializeUtil {

    public static <T> byte[] serialize(T object, Class<T> clazz){
        Serializer serializer = SerializerManager.getSerializer(OssConfigs.DEFAULT_SERIALIZER);
        return serializer.serialize(object, clazz);
    }

    public static <T> T deserialize(byte[] serialized, Class<T> clazz){
        Serializer serializer = SerializerManager.getSerializer(OssConfigs.DEFAULT_SERIALIZER);
        return serializer.deserialize(serialized, clazz);
    }
}
