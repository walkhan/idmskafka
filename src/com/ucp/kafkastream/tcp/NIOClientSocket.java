package com.ucp.kafkastream.tcp;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class NIOClientSocket {
    final static Logger LOG = Logger.getLogger(NIOClientSocket.class) ;

    public static void main(String[] args) throws Exception{
        //创建SocketChannel
        SocketChannel socketChannel = SocketChannel.open() ;
        //连接服务器
        socketChannel.connect(new InetSocketAddress("localhost",8989)) ;
        //写数据
        String msg = "我是客户端~" ;
        ByteBuffer buffer = ByteBuffer.allocate(1024) ;
        buffer.put(msg.getBytes()) ;
        buffer.flip() ;
        socketChannel.write(buffer) ;
        socketChannel.shutdownOutput() ;

        //读数据
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        int len = 0 ;
        while(true){
            buffer.clear() ;
            len = socketChannel.read(buffer) ;
            if(len == -1){
                break;
            }
            buffer.flip() ;
            while(buffer.hasRemaining()){
                bos.write(buffer.get()) ;
            }
        }
        System.out.println("客户端收到:" + new String(bos.toByteArray()));
        socketChannel.close();
    }
}
