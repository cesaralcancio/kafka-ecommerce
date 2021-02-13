import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello World!!!");
        for (var i = 0; i < 100; i++) {
            runNewOrder();
        }
    }

    private static void runNewOrder() throws InterruptedException, ExecutionException {
        var producer = new KafkaProducer<String, String>(properties());

        var topicNewOrder = "ECOMMERCE_NEW_ORDER";
        var topicSendEmail = "ECOMMERCE_SEND_EMAIL";

        var key = UUID.randomUUID().toString();
        var value = key + ",13213,65464,8797,123";
        var emailValue = "Thank you. We are processing your order.";

        var record = new ProducerRecord<String, String>(topicNewOrder, key, value);
        var emailRecord = new ProducerRecord<String, String>(topicSendEmail, key, emailValue);

        producer.send(record, getCallback()).get();
        producer.send(emailRecord, getCallback()).get();
    }

    private static Callback getCallback() {
        return (data, ex) -> {
            if (ex != null) {
                System.out.println(ex);
                return;
            }
            String msg = data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp();
            System.out.println("Message sent: " + msg);
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
