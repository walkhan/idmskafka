package com.ucp.kafkastream.util;

import org.apache.log4j.Logger;

import java.io.*;

/**
 * 对象序列化为byte数组
 *
 * @param
 * @return
 */

public class Bean2KafkaUtils {
    final static Logger LOG = Logger.getLogger(Bean2KafkaUtils.class) ;
    private String encoding = "UTF-8" ;

    public static byte[] ObjectToByte(Object obj){
        if(obj == null){
            return null ;
        }
        byte[] bytes = null ;
        ObjectOutputStream objectOutputStream = null ;
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        try {
            objectOutputStream.writeObject(obj);
            objectOutputStream.flush();
            bytes = bos.toByteArray() ;
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if(bos == null){
                try {
                    bos.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            if(objectOutputStream == null){
                try {
                    objectOutputStream.close();
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return bytes ;
    }

    /**
     * 字节数组转为Object对象
     *
     * @param bytes
     * @return
     */

    public static Object ByteArayToObject(byte[] bytes){
        Object readObject = null ;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes) ;
            ObjectInputStream inputStream = new ObjectInputStream(in) ;
            readObject = inputStream.readObject() ;
        } catch (Exception e){
            e.printStackTrace();
        }
        return readObject ;
    }

    public static boolean isStringNullOrEmpty(String value){
        return (null == value) || " ".equals(value) ;
    }
}
