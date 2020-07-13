package Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


/**
 * Simple Consumer Example
 */
public class Consumer1 {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.88.13:9092,192.168.88.14:9092,192.168.88.15:9092");
        //setting group id
        props.setProperty("group.id", "testGroupOne");
        //Setting enable.auto.commit means that offsets are committed
        // automatically with a frequency controlled by the config auto.commit.interval.ms.
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        //The deserializer settings specify how to turn bytes into objects.
        // For example, by specifying string deserializers, we are saying that our record's key and value will just be simple strings.
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //setting topic which the consumer subscribing
        consumer.subscribe(Arrays.asList("testProducer-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
