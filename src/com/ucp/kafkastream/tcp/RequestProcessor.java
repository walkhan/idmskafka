package com.ucp.kafkastream.tcp;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * 读操作的工具类
 * */

public class RequestProcessor {
    final static Logger LOG = Logger.getLogger(RequestProcessor.class) ;
    //构造线程池
    private static ExecutorService executorService = Executors.newFixedThreadPool(10) ;
    public static void ProcessorRequest(final SelectionKey selectionKey, final Selector selector){
        //获得线程并执行
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("开始读......");
                    SocketChannel readSocketChannel = (SocketChannel) selectionKey.channel();
                    //I/O读数据操作
                    ByteBuffer byteBuffer = ByteBuffer.allocate(1024) ;
                    ByteArrayOutputStream boos = new ByteArrayOutputStream() ;
                    int len = 0 ;
                    while(true){
                        byteBuffer.clear() ;
                        len = readSocketChannel.read(byteBuffer) ;
                        if(len == -1){
                            break;
                            }
                        byteBuffer.flip() ;
                        while(byteBuffer.hasRemaining()){
                            boos.write(byteBuffer.get());
                        }
                    }
                    System.out.println("服务器端接收到的数据：" + new String(boos.toByteArray()));
                    //将数据添加到key中
                    selectionKey.attach(boos) ;
                    //将注册写操作添加到队列中
                    NIOServerSocket.addWriteQueen(selectionKey);
//                    System.out.println("开始注册------");
                    //注册写操作
/*                    readSocketChannel.register(selector,SelectionKey.OP_WRITE) ;
                    System.out.println("注册完成-------");*/
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }) ;
    }

}
