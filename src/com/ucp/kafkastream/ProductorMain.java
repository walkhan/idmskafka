package com.ucp.kafkastream;

import com.google.gson.JsonObject;
import com.ucp.kafkastream.dto.productordto.SingleKafkaProductor;
import com.ucp.kafkastream.json.CreateJson;

public class ProductorMain {
    public static void main(String[] args) {
        CreateJson createJson1 = new CreateJson() ;
        JsonObject ss = createJson1.createJsonObject() ;
        Thread sendThread = new Thread( new Runnable() {
            @Override
            public void run() {
                while(true){
                    SingleKafkaProductor.getInstance().sendMessage(ss);
                    try {
                        Thread.currentThread().sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }) ;
        sendThread.start();
    }
}
