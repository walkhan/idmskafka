package com.ucp.kafkastream.redisdata;

import java.io.Serializable;
import java.util.List;

public class RedisStoredInfo implements Serializable {
    private String serviceName ;
    private String serviceId ;
    private String storeType ;
    private String expireTime ;
    private List<String>  valueList ;

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public String getStoreType() {
        return storeType;
    }

    public void setExpireTime(String expireTime) {
        this.expireTime = expireTime;
    }

    public String getExpireTime() {
        return expireTime;
    }

    public void setValueList(List<String> valueList) {
        this.valueList = valueList;
    }

    public List<String> getValueList() {
        return valueList;
    }
}
