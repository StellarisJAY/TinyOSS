package com.jay.oss.proxy.entity;

import lombok.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/03 15:27
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Result implements Serializable {
    private String message;
    private Map<String, Object> data = new HashMap<>();

    public void putData(String name, Object value){
        data.put(name, value);
    }
}
