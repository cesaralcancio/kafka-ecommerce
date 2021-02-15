package br.com.cesar.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.concurrent.ExecutionException;

public class CreateUserService {

    public static void main(String[] args) {
        var topic = "ECOMMERCE_NEW_ORDER";
        var createUserService = new CreateUserService();
        try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                topic,
                createUserService::parse,
                Order.class)) {
            service.run();
        }
    }

    private KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("Processing new order, checking for new user.");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println("New user checked!");
    }
}
