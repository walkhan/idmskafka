package com.ucp.kafkastream.dto.consumerdto;

import java.util.List;

/**
 * @program: com.ucp.kafka
 * @description: ${description}
 * @author: hanfei
 * @create: 2018-11-02 19:19
 **/
public class ConsumerConfigBean {
    private String brokerList ;
    private List<String> topics ;
    private String groupId ;
    private String clientId ;
    private String reSet ;
    private String commmits ;
    private int records ;
    public ConsumerConfigBean(String brokerList,List<String> topics,String groupId,String clientId,String reSet, String commmits, int records ){
        if(topics.isEmpty()){
            throw  new IllegalArgumentException("topic is not empty List!") ;
        } else if(brokerList == null){
            throw  new IllegalArgumentException("bootstrap servers must not be null value") ;
        } else{
            this.topics = topics ;
            this.brokerList = brokerList ;
            this.groupId = groupId ;
            this.clientId = clientId ;
            this.reSet = reSet ;
            this.commmits = commmits ;
            this.records = records ;
        }
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setReSet(String reSet) {
        this.reSet = reSet;
    }

    public String getReSet() {
        return reSet;
    }

    public void setCommmits(String commmits) {
        this.commmits = commmits;
    }

    public String getCommmits() {
        return commmits;
    }

    public void setRecords(int records) {
        this.records = records;
    }

    public int getRecords() {
        return records;
    }
}
