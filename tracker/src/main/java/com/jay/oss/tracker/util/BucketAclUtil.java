package com.jay.oss.tracker.util;

import com.jay.dove.transport.command.CommandCode;
import com.jay.oss.common.acl.Acl;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.entity.bucket.Bucket;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.AccessTokenUtil;
import com.jay.oss.tracker.meta.BucketManager;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  Bucket ACL检验工具
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 11:17
 */
@Slf4j
public class BucketAclUtil {
    /**
     * 检查访问权限，并创建回复报文
     * @param bucketName 桶
     * @param token token
     * @param accessMode 访问模式，{@link BucketAccessMode}
     * @return {@link FastOssCommand} 回复报文
     */
    public static CommandCode checkAuthorization(BucketManager bucketManager, String bucketName, String token, BucketAccessMode accessMode){
        // 获取桶
        Bucket bucket = bucketManager.getBucket(bucketName);
        CommandCode code;
        // 桶是否存在
        if(bucket != null){
            String accessKey = bucket.getAccessKey();
            String secretKey = bucket.getSecretKey();
            Acl acl = bucket.getAcl();
            // 检查 READ 权限
            if(accessMode == BucketAccessMode.READ){
                // PRIVATE acl下需要检查token
                if(acl == Acl.PRIVATE && !AccessTokenUtil.checkAccessToken(accessKey, secretKey, token)){
                    code = FastOssProtocol.ACCESS_DENIED;
                }else{
                    code = FastOssProtocol.SUCCESS;
                }
            }
            // 检查 WRITE权限
            else if(accessMode == BucketAccessMode.WRITE){
                // 非PUBLIC_WRITE acl下需要检验token
                if(acl != Acl.PUBLIC_WRITE && !AccessTokenUtil.checkAccessToken(accessKey, secretKey, token)){
                    code = FastOssProtocol.ACCESS_DENIED;
                }else{
                    code = FastOssProtocol.SUCCESS;
                }
            }
            // 检查WRITE_ACL权限
            else{
                if(!AccessTokenUtil.checkAccessToken(accessKey, secretKey, token)){
                    code = FastOssProtocol.ACCESS_DENIED;
                }else{
                    code = FastOssProtocol.SUCCESS;
                }
            }

        }else{
            // 桶不存在，返回NOT_FOUND
            code = FastOssProtocol.NOT_FOUND;
        }
        return code;
    }
}
