package com.ucp.kafkastream.dto.productordto;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class SingleProductor{
    final static Logger LOG=Logger.getLogger(SingleProductor.class) ;
    private ProductorConfigBean productorConfigBean = new ProductorConfigBean() ;
    private static SingleProductor _instance = null ;
    private String productorConfigPath= System.getProperty("user.dir")
            + File.separator + "manager" + File.separator + "productor.properties" ;
    private KafkaProducer<String,Object> kafkaProducer = null ;
    private BlockingDeque<Object> _messageChche = new LinkedBlockingDeque<Object>(Integer.MAX_VALUE) ;

    //构造私有化，禁止实例化
    private SingleProductor(){  }

    /**
     * 单例模式,kafkaProducer是线程安全的,可以多线程共享一个实例
     *
     * @return
     */

    public static SingleProductor getInstance(){
        if( _instance == null){
            synchronized(SingleProductor.class){
                if(_instance == null){
                    _instance = new SingleProductor() ;
                    Properties properties = new Properties() ;
                    properties.put("bootstrap.servers", _instance.productorConfigBean.getBroker() ) ;
                    properties.put("topics",_instance.productorConfigBean.getTopic()) ;
                    properties.put("key.serializer",_instance.productorConfigBean.getKeySerializer()) ;
                    properties.put("value.serializer",_instance.productorConfigBean.getValueSerializer()) ;
                    properties.put("acks",_instance.productorConfigBean.getAcks()) ;
                    properties.put("batch.size",_instance.productorConfigBean.getBatchSize()) ;
                    properties.put("linger.ms",_instance.productorConfigBean.getLingerMS()) ;
                    properties.put("retries",_instance.productorConfigBean.getRetries()) ;
                    _instance.kafkaProducer = new KafkaProducer<String, Object>(properties) ;
                    if(LOG.isDebugEnabled()){
                        LOG.debug("SingleProductor init KafkaProducer success ! " + _instance.productorConfigBean) ;
                    }
                }
            }
        }
        return _instance ;
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
            kafkaProducer = new KafkaProducer<String, Object>(properties) ;
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
    /**
     * 通过kafkaProducer发送消息(队列)
     * @param messages
     */

    public void sendMessage(Object messages){
        _messageChche.offer(messages) ;
    }


    /**
     * 通过kafkaProducer发送消息
     * @param message
     *            具体消息值
     */

    public void sendKafkaMessage(Object message) throws InterruptedException {
        /**
         * 1、如果指定了某个分区,会只讲消息发到这个分区上 2、如果同时指定了某个分区和key,则也会将消息发送到指定分区上,key不起作用
         * 3、如果没有指定分区和key,那么将会随机发送到topic的分区中 4、如果指定了key,那么将会以hash<key>的方式发送到分区中
         */
        _messageChche.take() ;
        ProducerRecord<String,Object> record = new ProducerRecord<>(_instance.productorConfigBean.getTopic(),
                message) ;
        getInstance().kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(null != e){
                    LOG.error("kafka发送消息失败:" + e.getMessage(),e) ;
                    try {
                        retrySendMesage(message) ;
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }) ;
        getInstance().kafkaProducer.flush();
    }

    /**
     * 当kafka消息发送失败后,重试
     *
     * @param retryMessage
     */
    public  void retrySendMesage(Object retryMessage) throws InterruptedException {
        _messageChche.take() ;
        ProducerRecord<String,Object> record = new ProducerRecord<>(_instance.productorConfigBean.getTopic(),
                retryMessage) ;
        for (int i = 0; i < _instance.productorConfigBean.getRetry(); i++) {
            try {
                getInstance().kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    }
                });
                return ;
            } catch (Exception e){
                LOG.error("kafka发送消息失败:" + e.getMessage(), e);
                retrySendMesage(retryMessage) ;
            }

        }
    }

    /**
     * kafka实例销毁
	 */

    public void close(){
        if(null != kafkaProducer){
            kafkaProducer.close();
        }
    }
}
