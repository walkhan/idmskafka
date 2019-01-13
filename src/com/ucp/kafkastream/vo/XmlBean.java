package com.ucp.kafkastream.vo;

public class XmlBean {
    private String service;
    private String times;
    private String flag;
    private String cleanTime ;

    public void setService(String service) {
        this.service = service;
    }

    public String getService() {
        return service;
    }

    public void setTimes(String times) {
        this.times = times;
    }

    public String getTimes() {
        return times;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getFlag() {
        return flag;
    }

    public void setCleanTime(String cleanTime) {
        this.cleanTime = cleanTime;
    }

    public String getCleanTime() {
        return cleanTime;
    }


}