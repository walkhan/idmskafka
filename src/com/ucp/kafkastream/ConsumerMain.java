package com.ucp.kafkastream ;

import com.ucp.kafkastream.manager.ConsumerManager;

public class ConsumerMain {
    public static void main(String[] args) {
	// write your code here
        ConsumerManager.getInstance().getKafkaConsumer().start();
    }
}
