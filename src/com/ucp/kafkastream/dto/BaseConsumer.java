package com.ucp.kafkastream.dto;

/**
 * @program: com.ucp.kafka
 * @description: ${description}
 * @author: hanfei
 * @create: 2018-11-02 21:15
 **/
public abstract class BaseConsumer implements Runnable {
    @Override
    public void run(){ }

    public  void start(){  }

    public void shutdown(){}
}
