package com.ucp.kafkastream.dto.consumerdto;

import com.ucp.kafkastream.manager.ConsumerManager;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: com.ucp.kafka
 * @description: ${description}
 * @author: hanfei
 * @create: 2018-11-02 19:14
 **/
public class ConsumerThreadHandle<K,V>  {
    final static Logger LOG = LoggerFactory.getLogger(ConsumerThreadHandle.class) ;
    private KafkaConsumer<K,V> consumer ;
    private ConsumerRecords<K,V> records ;
    private Map<TopicPartition,OffsetAndMetadata> offsets = new HashMap<>() ;
    private ConsumerManager executor;
    private ExecutorService executorService ;
    private Executor executors ;
    private Map<TopicPartition,Long> partitionMap = new ConcurrentHashMap<>();
    private AtomicBoolean isStarted = new AtomicBoolean(false) ;
    private AtomicInteger _pollCount = new AtomicInteger(0) ;


    public ConsumerThreadHandle(Properties config){
        System.setProperty("java.security.auth.login.config", "/home/kafka/kafka_client_jaas.conf");
        System.setProperty("java.security.krb5.conf", "/home/kafka/krb5.conf");
        System.setProperty( "javax.security.auth.useSubjectCredsOnly","false" ) ;
        List<String> topic = Collections.singletonList(config.getProperty("consumer.topics"));
        String brokerList = config.getProperty("consumer.bootstrap_servers") ;
        String groupId = config.getProperty("consumer.group_id") ;
        String clientId = config.getProperty("consumer.client_id") ;
        String reSet = config.getProperty("auto.offset.reset") ;
        String commmits = config.getProperty("enable.auto.commit") ;
        int records = Integer.parseInt(config.getProperty("max.poll.records")) ;
        Properties props = new Properties() ;
        props.put("bootstrap.servers",brokerList) ;
        props.put("group.id",groupId) ;
        props.put("client.id",clientId) ;
        props.put("auto.offset.reset",reSet) ;
        props.put("enable.auto.commit",commmits) ;
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");
        consumer = new KafkaConsumer<K,V>(props) ;
        consumer.subscribe(topic, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                for(TopicPartition partition: partitions){
                    if(offsets.isEmpty()){
                        long commitStartTime = System.currentTimeMillis() ;
                        //记录分区和它的偏移量
                        partitionMap.put(partition,consumer.position(partition)) ;
                        //提交偏移量
                        consumer.commitSync(offsets);
                        long commitEndTime = System.currentTimeMillis() ;
                        if(LOG.isDebugEnabled()){
                            LOG.info("topic:{},partition:{}",partition.topic(),partition.partition());
                            LOG.info("batch commit time in millis:",(commitStartTime - commitEndTime));
                        }
                    }
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //设置分区的偏移量
                for(TopicPartition partition:partitions){
                    if(partitionMap.get(partition) != null){
                        consumer.seek(partition,partitionMap.get(partition));
                    }
                }
                offsets.clear() ;
            }
        });
    }


    /*
     * 消费主方法
     * threadnumber 线程池中的线程数
     * */

    public  void consume(){
        try {
            ConsumerRecords<K,V> records = consumer.poll(Long.MAX_VALUE) ;
            if(LOG.isDebugEnabled()){
                LOG.debug("redis total poll count:" + _pollCount.addAndGet(records.count()));
            }
            LOG.info("poll count:{}",records.count()) ;
            if(! records.isEmpty()){
                ConsumerWorker  worker = new ConsumerWorker<>(records,offsets) ;
                ConsumerManager.getInstance().getExecutor().submit(worker) ;
            }
            commitOffsets() ;
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            commitOffsets();
        }

    }


    public void start() {
        if(isStarted.compareAndSet( false,true )){
           Runnable runner = () -> {
                while(isStarted.get()){
                    try {
                        if(ConsumerManager.getInstance().getThreadPoolMaxIdleCount() < 10){
                            Thread.currentThread().sleep(3000) ;
                            continue;
                        }
                        consume();
                        Thread.currentThread().sleep(3000);
                    } catch(Exception e){
                        e.printStackTrace();
                    }
                }
           };
           ConsumerManager.getInstance().getExecutor().submit(runner) ;

        }

    }

    private void commitOffsets(){
        //尽量降低对synchronized快对offsets的锁定时间
        Map<TopicPartition, OffsetAndMetadata> unmodfiedMap ;
        synchronized(offsets){
            if(offsets.isEmpty()){
                return ;
            }
            unmodfiedMap = Collections.unmodifiableMap(new HashMap<>(offsets)) ;
            offsets.clear() ;
        }
        consumer.commitSync(unmodfiedMap);
    }

    //关闭消费者
    public void shutdown() {
        try {
            if(consumer != null){
                consumer.close();
            }
            if(executor != null){
                executor.shutdown() ;
            }
            if( ! executorService.awaitTermination(10,TimeUnit.SECONDS)){
                System.out.println("Timeout");
            }
        } catch(InterruptedException  ignored){
            Thread.currentThread().interrupt();
        }
    }
}
