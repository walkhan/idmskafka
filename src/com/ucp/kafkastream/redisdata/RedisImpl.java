package com.ucp.kafkastream.redisdata;

import com.ucp.kafkastream.util.RedisUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class RedisImpl {
    final static Logger LOG = Logger.getLogger(RedisImpl.class) ;
    private static RedisImpl _instance = null ;
    private  RedisImpl(){}

    public static RedisImpl getInstance(){
        if(_instance == null){
            _instance = new RedisImpl() ;
        }
        return _instance ;
    }


    public void putToRedis(String sdo_key,Object sdo_value) throws InterruptedException {
        RedisUtils redisUtils = RedisUtils.getInstance() ;
        Map<String,Object> map = new HashMap<>() ;
        map.put(sdo_key,sdo_value) ;
        redisUtils.getRedis_msg_container().put(map) ;
    }



    public static String bytes2HexStr(byte[] arr){
        int iLen = arr.length ;
        StringBuilder sb = new StringBuilder(iLen * 2) ;
        for (int i = 0; i < iLen ; i++) {
            int intTmp = arr[i] ;
            while(intTmp < 0){
                intTmp += 256 ;
            }
            if(intTmp < 16){
                sb.append("0") ;
            }
            sb.append(Integer.toString(intTmp,16)) ;
        }
        return sb.toString() ;
    }
}
