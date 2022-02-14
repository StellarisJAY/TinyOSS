package com.jay.oss.common.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/14 14:09
 */
public class Banner {
    public static void printBanner(){
        ClassLoader classLoader = Banner.class.getClassLoader();
        try(InputStream inputStream = classLoader.getResourceAsStream("banner");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))){
            String line;
            while((line = reader.readLine()) != null){
                System.out.println(line);
            }
        }catch (IOException ignored){

        }
    }
}
