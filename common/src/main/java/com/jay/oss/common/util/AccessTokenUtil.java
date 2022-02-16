package com.jay.oss.common.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * <p>
 *  通过AccessKey和SecretKey生成token
 *  双重摘要算法加密：MD5(MD5(sk + endTimestamp) + ak)
 *
 *  token 组成： start:end;alg=algorithm;sig=..........
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 14:51
 */
public class AccessTokenUtil {
    /**
     * 根据ak和sk以及token有效期计算token
     * @param accessKey AccessKey
     * @param secretKey secretKey
     * @param algorithm 摘要算法
     * @param period token有效时间
     * @throws NoSuchAlgorithmException 无效的算法
     * @return byte[] token
     */
    public static String getToken(String accessKey, String secretKey, String algorithm, long period) throws NoSuchAlgorithmException {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + period;

        String signature = digest(accessKey, secretKey, algorithm, endTime);
        return startTime +
                ":" +
                endTime + ";" +
                "alg=" + algorithm + ";" +
                "sig=" + signature;
    }

    /**
     * 生成摘要字符串
     * @param accessKey ak
     * @param secretKey sk
     * @param algorithm 摘要算法
     * @param endTime 时间戳
     * @return HexString
     * @throws NoSuchAlgorithmException 无效算法
     */
    public static String digest(String accessKey, String secretKey, String algorithm, long endTime) throws NoSuchAlgorithmException {
        MessageDigest instance = MessageDigest.getInstance(algorithm);
        instance.update((secretKey + endTime).getBytes(StandardCharsets.UTF_8));
        byte[] encrypt1 = instance.digest();
        instance.update((new String(encrypt1, StandardCharsets.UTF_8) + accessKey).getBytes(StandardCharsets.UTF_8));
        byte[] encryption = instance.digest();

        StringBuilder builder = new StringBuilder();
        for (byte b : encryption) {
            String hex = Integer.toHexString((int)b & 0xff);
            if(hex.length() == 1){
                builder.append("0");
            }
            builder.append(hex);
        }
        return builder.toString();
    }


    /**
     * 检查AccessToken是否有效
     * @param accessKey ak
     * @param secretKey sk
     * @param token Sting token
     * @return boolean
     */
    public static boolean checkAccessToken(String accessKey, String secretKey, String token){
        // 切分token，得到三部分：时效、摘要算法、签名
        String[] parts = token.split(";");
        if(parts.length != 3){
            return false;
        }
        String timeParts = parts[0];
        String algPart = parts[1];
        String sigPart = parts[2];
        int i = timeParts.indexOf(":");
        if(i == -1){
            return false;
        }
        try{
            long startTime = Long.parseLong(timeParts.substring(0, i));
            long endTime = Long.parseLong(timeParts.substring(i + 1));
            if(System.currentTimeMillis() >= endTime){
                return false;
            }
            int algI = algPart.indexOf("alg=");
            int sigI = sigPart.indexOf("sig=");
            String algorithm = algPart.substring(algI + 4);
            String signature = sigPart.substring(sigI + 4);
            String digest = digest(accessKey, secretKey, algorithm, endTime);
            return digest.equals(signature);
        }catch (Exception e){
            return false;
        }
    }
}
