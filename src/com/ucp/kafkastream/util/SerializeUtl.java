package com.ucp.kafkastream.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializeUtl {

    public static byte[] ObjToSerialize(Object obj){
        ObjectOutputStream oos = null ;
        byte [] bytes = null ;
        ByteArrayOutputStream byteArrayOutputStream = null ;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream() ;
            oos = new ObjectOutputStream(byteArrayOutputStream) ;
            oos.writeObject(obj);
            oos.flush();
            bytes = byteArrayOutputStream.toByteArray() ;
        } catch (Exception e){
            e.printStackTrace();
        }
        return bytes ;
    }

    public static Object deSerialize(byte[] bytes){
        ByteArrayInputStream in = null ;
        in = new ByteArrayInputStream(bytes) ;
        try {
            ObjectInputStream objectInputStream = new ObjectInputStream(in) ;
            return objectInputStream.readObject() ;
        } catch (Exception e){
            e.printStackTrace();
        }

        return bytes ;
    }
}
