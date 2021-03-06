package br.com.cesar.ecommerce;

import br.com.cesar.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        var topic = Pattern.compile("ECOMMERCE.*");
        System.out.println("Reading type: " + String.class.getName());

        try (var service = new KafkaService(LogService.class.getSimpleName(),
                topic,
                logService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("Log processed!");
    }
}
