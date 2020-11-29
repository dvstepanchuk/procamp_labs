package consumer;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.time.Duration;
import java.util.*;

public class Consumer {
    public static void main(String[] args) throws ParseException {

        String topic = "btcusd-transaction-topic";
        KafkaConsumer<String, String> consumer = createConsumer();

        consumer.subscribe(Collections.singleton(topic));
        computeTop10(consumer);

    }

    private static void computeTop10(KafkaConsumer consumer) throws ParseException {
        List<Pair> top10 = new ArrayList<>();
        while(true) {
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    String recordValue = record.value();
                    JSONObject object = (JSONObject) new JSONParser().parse(recordValue);
                    JSONObject data = (JSONObject) object.get("data");
                    if(data == null || data.get("price") == null) {
                        System.out.println("Invalid message: " + recordValue);
                        continue;
                    }
                    Double price = Double.valueOf(data.get("price").toString());

                    top10.add(new ImmutablePair(price, recordValue));

                }
                top10.sort(Comparator.reverseOrder());
                if (top10.size() > 10) {
                    top10 = top10.subList(0, 10);
                }
                consumer.commitSync();

                System.out.println("TOP 10 records:");
                top10.forEach(i -> System.out.println("Price: " + i.getKey() + ", BitcoinTransaction: " + i.getValue()));
                System.out.println(" ");
            }
        }

    }

    private static KafkaConsumer<String, String> createConsumer() {
        String bootstrapServers = "localhost:9092";
        String groupId = "123";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        return consumer;
    }


}
