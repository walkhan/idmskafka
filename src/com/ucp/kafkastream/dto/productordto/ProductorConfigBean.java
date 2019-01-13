package com.ucp.kafkastream.dto.productordto;


import org.apache.log4j.Logger;

public class ProductorConfigBean {
    final static Logger LOG= Logger.getLogger(ProductorConfigBean.class) ;
    private String broker = "" ;
    private String topic = "" ;
    private String keySerializer = "" ;
    private String valueSerializer = "" ;
    private String acks = "" ;
    private int batchSize ;
    private long lingerMS  ;
    private int retries ;
    private int retry ;

    /*无参构造*/

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public String getBroker() {
        return broker;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public String getAcks() {
        return acks;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setLingerMS(long lingerMS) {
        this.lingerMS = lingerMS;
    }

    public long getLingerMS() {
        return lingerMS;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public int getRetry() {
        return retry;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder() ;
        sb.append("kafka config info:") ;
        sb.append(broker) ;
        sb.append(topic) ;
        sb.append(keySerializer) ;
        sb.append(valueSerializer) ;
        sb.append(acks) ;
        sb.append(batchSize) ;
        sb.append(lingerMS) ;
        sb.append(retries) ;
        return sb.toString();
    }

}
