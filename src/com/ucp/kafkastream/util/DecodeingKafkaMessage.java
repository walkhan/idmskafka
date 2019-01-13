package com.ucp.kafkastream.util;

import org.apache.log4j.Logger;

import org.apache.kafka.common.serialization.Deserializer ;

import java.util.Map;

/**
 * 反序列化对象
 *
 * @param
 * @return
 */
public class DecodeingKafkaMessage implements Deserializer<Object> {
    final static Logger LOG = Logger.getLogger(DecodeingKafkaMessage.class) ;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return Bean2KafkaUtils.ByteArayToObject(data);
    }

    @Override
    public void close() { }
}
