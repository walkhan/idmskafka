package test.productor;

import org.apache.kafka.clients.producer.*;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleThreadProductorSync {
    public static void main(String[] args) {
            Properties prop = initProperties() ;
            String topicName = "fdw" ;
            Producer<String,String> producer = new KafkaProducer <String, String>(prop) ;
            Date date = new Date() ;
            AtomicInteger _sendCount = new AtomicInteger(0) ;
            for (int i = 0; i < 1000; i++) {
                try {
                    ProducerRecord<String,String> sendMsg = new ProducerRecord <String,String>(topicName,
                            Integer.toString(i),date.toString()) ;
                   RecordMetadata metadata = producer.send(sendMsg).get() ;
                    System.out.println( _sendCount.addAndGet(1) + "-" + sendMsg.key() + "," + sendMsg.value()
                            + ", "  + metadata.offset());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } finally {
                    producer.flush();
                    producer.close();
                }
            }


    }

    public static Properties initProperties(){
        Properties properties = new Properties() ;
        properties.put("bootstrap.servers","fdw1:9092,fdw2:9092,fdw3:9092") ;
        properties.put("acks","all") ;
        properties.put("retries","10") ;
        properties.put("batch.size","16432") ;
        properties.put("linger.ms","50") ;
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer") ;
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("enable.idempotence",true) ;
        properties.put("client.id","ucp_kafka") ;
        return properties ;
    }
}
