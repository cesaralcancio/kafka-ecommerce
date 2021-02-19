package br.com.cesar.ecommerce.consumer;

import br.com.cesar.ecommerce.Message;
import br.com.cesar.ecommerce.dispatcher.KafkaDispatcher;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction<T> parse;
    private final String groupId;

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(groupId, parse, properties);
        consumer.subscribe(topic);
    }

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse) {
        this(groupId, parse, Map.of());
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse) {
        this(groupId, parse, Map.of());
        consumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction<T> parse, Map<String, String> properties) {
        this.groupId = groupId;
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(properties));
    }

    public void run() {
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
                        sendToDeadLetter(record);
                    }
                }
            }
        }
    }

    private void sendToDeadLetter(ConsumerRecord<String, Message<T>> record) {
        try (var dispatcher = new KafkaDispatcher<>()) {
            var message = record.value();
            dispatcher.sendAndWait("ECOMMERCE_DEADLETTER",
                    message.getCorrelationId().continueWith("DeadLetter"),
                    message.getCorrelationId().getId(),
                    new GsonBuilder().create().toJson(message));

        } catch (Exception ex) {
            // if fail, finish the system
            ex.printStackTrace();
            throw new RuntimeException(ex);
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
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
