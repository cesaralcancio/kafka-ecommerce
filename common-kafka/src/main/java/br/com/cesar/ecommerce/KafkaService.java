package br.com.cesar.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction<T> parse;
    private final String groupId;

    KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(groupId, parse, properties);
        consumer.subscribe(topic);
    }

    KafkaService(String groupId, String topic, ConsumerFunction<T> parse) {
        this(groupId, parse, Map.of());
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse) {
        this(groupId, parse, Map.of());
        consumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction<T> parse, Map<String, String> properties) {
        this.groupId = groupId;
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(properties));
    }

    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("|---------------------------------------|");
                System.out.println("Found " + records.count() + " records.");
                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private Properties properties(Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
