package com.ucp.kafkastream.dto.productordto;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 另外一种解法
 */

public class SingleKafkaProductor {
    private final  static Logger LOG= Logger.getLogger(SingleKafkaProductor.class) ;
    private KafkaProducer<String,Object> producer = null ;
    private BlockingDeque<Object> _messageChe = new LinkedBlockingDeque <>(Integer.MAX_VALUE) ;
    private static SingleKafkaProductor _instance = null ;
    private ProductorConfigBean productorConfigBean = new ProductorConfigBean() ;
    private String productorConfigPath= System.getProperty("user.dir")
            + File.separator + "manager" + File.separator + "productor.properties" ;

    //构造函数私有化
    private SingleKafkaProductor(){
        initconfig();
        //创建缓冲队列消息线程
        Thread productorThread = new Thread(createCacheMsgHandle()) ;
        productorThread.setName("SingleKafkaProductor_thread");
        productorThread.start();

    }

    public static SingleKafkaProductor getInstance(){
        if(_instance == null){
            synchronized(SingleKafkaProductor.class){
                if(_instance == null){
                    _instance = new SingleKafkaProductor() ;
                    Properties properties = new Properties( ) ;
                    properties.put("bootstrap.servers",_instance.productorConfigBean.getBroker()) ;
                    properties.put("topics",_instance.productorConfigBean.getTopic()) ;
                    properties.put("key.serializer",_instance.productorConfigBean.getKeySerializer()) ;
                    properties.put("value.serializer",_instance.productorConfigBean.getValueSerializer()) ;
                    properties.put("acks",_instance.productorConfigBean.getAcks()) ;
                    properties.put("batch.size",_instance.productorConfigBean.getBatchSize()) ;
                    properties.put("linger.ms",_instance.productorConfigBean.getLingerMS()) ;
                    properties.put("retries",_instance.productorConfigBean.getRetries()) ;
                   _instance.producer = new KafkaProducer<String, Object>(properties) ;
                   if(LOG.isDebugEnabled()){
                       LOG.debug("SingleKafkaProductor init KafkaProducer sucess !" + _instance.productorConfigBean);
                   }
                }
            }
        }
        return _instance ;
    }

    public void sendMessage(Object sendObj) {
        _messageChe.offer(sendObj) ;
    }

    public Runnable createCacheMsgHandle(){
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                while(true){
                    try {
                        Object sendObj = _messageChe.take() ;
                        ProducerRecord<String,Object> record = new ProducerRecord <>(_instance.productorConfigBean.getTopic(),sendObj) ;
                        getInstance().producer.send(record) ;
                        getInstance().producer.flush();
                        if(LOG.isDebugEnabled()){
                            LOG.debug("Kafka message already send Message:" + record) ;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } ;
        return runnable ;
    }

    /**
     * Kafka生产者进行初始化
     */
    public void initconfig()  {
        File configFile = new File(productorConfigPath) ;
        Properties properties = new Properties() ;
        InputStream in = null ;
        try {
            in = new FileInputStream(configFile) ;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            properties.load(in) ;
            String broker = properties.getProperty("bootstrap.servers") ;
            productorConfigBean.setBroker(broker);
            String topics = properties.getProperty("topics") ;
            productorConfigBean.setTopic(topics) ;
            String keySerializer = properties.getProperty("key.serializer") ;
            productorConfigBean.setKeySerializer(keySerializer);
            String valueSerializer = properties.getProperty("value.serializer") ;
            productorConfigBean.setValueSerializer(valueSerializer) ;
            String acks = properties.getProperty("acks") ;
            productorConfigBean.setAcks(acks) ;
            int batchSize = Integer.parseInt(properties.getProperty("batch.size"));
            productorConfigBean.setBatchSize(batchSize) ;
            long lingerMS = Long.parseLong(properties.getProperty("linger.ms")) ;
            productorConfigBean.setLingerMS(lingerMS) ;
            int retries = Integer.parseInt(properties.getProperty("retries")) ;
            productorConfigBean.setRetries(retries) ;
        } catch (IOException e) {
            LOG.error("kafkaProducer初始化失败:" + e.getMessage(), e) ;
        } finally {
            if(null != in){
                try {
                    in.close();
                }catch (IOException e){
                    LOG.error("kafkaProducer初始化失败:" + e.getMessage(), e) ;
                }
            }
        }
    }
}
