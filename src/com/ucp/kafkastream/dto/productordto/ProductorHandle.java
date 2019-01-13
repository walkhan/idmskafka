package com.ucp.kafkastream.dto.productordto;


import org.apache.log4j.Logger;

public class ProductorHandle implements Runnable {
    final static Logger LOG = Logger.getLogger(ProductorHandle.class) ;
    private String message ;
    public ProductorHandle(String message){
        this.message = message ;
    }
    @Override
    public void run() {
       while(true){
            SingleProductor singleProductor = SingleProductor.getInstance() ;
            singleProductor.initconfig();
           try {
               singleProductor.sendKafkaMessage("发送消息:" + message) ;
           } catch (InterruptedException e) {
               LOG.error("发送消息失败:" + e.getMessage() );
           }

       }
    }
}
