package br.com.cesar.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    KafkaDispatcher() {
        producer = new KafkaProducer<>(properties());
    }

    public RecordMetadata sendAndWait(String topic, CorrelationId correlationId, String key, T value) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = sendAsync(topic, correlationId, key, value);
        return future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, CorrelationId correlationId, String key, T value) {
        var message = new Message<>(correlationId, value);
        var record = new ProducerRecord<>(topic, key, message);
        return producer.send(record, this::callback);
    }

    private void callback(RecordMetadata data, Exception ex) {
        if (ex != null) {
            System.out.println(ex);
            return;
        }
        String msg = data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp();
        System.out.println("Message sent: " + msg);
    }

    private Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        //ref: https://kafka.apache.org/0100/documentation.html
        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
