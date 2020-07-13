package Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * Simple Consumer Example with Manual control offset
 * consume a batch of records and batch them up in memory
 */
public class ConsumerManualOffset {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.88.13:9092");
        //setting group id
        props.setProperty("group.id", "Manual1Group");
        //Setting enable.auto.commit means that offsets are committed
        // automatically with a frequency controlled by the config auto.commit.interval.ms.
        props.setProperty("enable.auto.commit", "false");
        //use new group id,from earliest offset
        props.setProperty("auto.offset.reset", "earliest");
//        props.setProperty("auto.commit.interval.ms", "1000");
        //The deserializer settings specify how to turn bytes into objects.
        // For example, by specifying string deserializers, we are saying that our record's key and value will just be simple strings.
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //setting topic which the consumer subscribing
        consumer.subscribe(Arrays.asList("testProducer-topic"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        int batchNum =1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
//                insertIntoDb(buffer);
                for (ConsumerRecord<String, String> record: buffer) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                consumer.commitSync();
                buffer.clear();
                //print the batch number
                System.out.printf("bach num : %d", batchNum++);
            }
        }
    }
}
