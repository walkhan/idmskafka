package com.ucp.kafkastream.util;

import org.apache.kafka.common.serialization.Serializer ;
import java.util.Map;

public class EncodeingKafkaMessage implements Serializer<Object> {

    @Override
    public void configure(Map<String, ?> config, boolean iskey) {

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return  Bean2KafkaUtils.ObjectToByte(data);
    }

    @Override
    public void close() {

    }
}
