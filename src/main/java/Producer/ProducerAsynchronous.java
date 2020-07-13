package Producer;

import org.apache.kafka.clients.producer.*;

import java.text.Format;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Invoke Future get() method as Asynchronous Producer
 */
public class ProducerAsynchronous {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.13:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            try {
                Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(
                        "testProducer-topic", Integer.toString(i), "Asynchronous Message:" + Integer.toString(i)), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        } else {
                            System.out.println(String.format("The Message's Topic is %s, offset is %d .", recordMetadata.topic(), recordMetadata.offset()));
                        }
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        producer.close();
    }
}
