package Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Invoke Future get() method as Synchronous Producer
 *
 */
public class ProducerSynchronous {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.13:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++){
            try {
                Future<RecordMetadata> future= producer.send(new ProducerRecord<String, String>("testProducer-topic", Integer.toString(i), "Message:"+Integer.toString(i)));
                RecordMetadata recordMetadata = future.get();
                System.out.println("Message MetaData:"+recordMetadata.toString());
            }catch (Exception e){
                e.printStackTrace();
            }

        }

        producer.close();
    }
}
