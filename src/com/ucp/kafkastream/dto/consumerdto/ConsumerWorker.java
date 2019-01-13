package com.ucp.kafkastream.dto.consumerdto;

import com.ucp.kafkastream.dto.BaseConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @program: com.ucp.kafka
 * @description: ${description}
 * @author: hanfei
 * @create: 2018-11-02 19:14
 **/
public class ConsumerWorker<K,V> extends BaseConsumer {
    private final Logger LOG = (Logger) LoggerFactory.getLogger(ConsumerWorker.class);
    private final ConsumerRecords<K,V> records ;
    private final Map<TopicPartition,OffsetAndMetadata> offsets ;
    public ConsumerWorker( ConsumerRecords<K,V> records, Map<TopicPartition,OffsetAndMetadata> offsets){
        this.offsets = offsets ;
        this.records = records ;
    }


    @Override
    public synchronized void run() {
        for(TopicPartition partition: records.partitions()){
            List<ConsumerRecord<K,V>> partitionRecords = records.records(partition) ;
            for(ConsumerRecord<K,V> record:partitionRecords){
                //插入消息处理逻辑，打印信息
                LOG.info("当前线程:" + Thread.currentThread().getName() + ","
                        + "偏移量:" + record.offset() + "," + "主题:"
                        + record.topic() + "," + "分区:" + record.partition()
                        + "," + "获取的消息:" + record.value()) ;
            }
            //上报位移信息
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset() ;
            synchronized(offsets){
                if(! offsets.containsKey(partition)){
                    offsets.put(partition,new OffsetAndMetadata(lastOffset + 1)) ;
                } else {
                    long curr = offsets.get(partition).offset() ;
                    if(curr <= lastOffset + 1){
                        offsets.put(partition,new OffsetAndMetadata(lastOffset + 1 )) ;
                    }
                }
            }
        }
    }
}
