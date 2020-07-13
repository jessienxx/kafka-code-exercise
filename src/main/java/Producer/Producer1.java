package Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Simple Producer Example
 */
public class Producer1 {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.13:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 300; i++){
            try {
                producer.send(new ProducerRecord<String, String>("testProducer-topic", Integer.toString(i), "Message:"+Integer.toString(i)));
            }catch (Exception e){
                e.printStackTrace();
            }

            System.out.println("i:"+i);
        }

        producer.close();
    }
}
