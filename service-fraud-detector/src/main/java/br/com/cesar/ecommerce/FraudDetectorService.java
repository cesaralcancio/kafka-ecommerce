package br.com.cesar.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main(String[] args) {
        var topic = "ECOMMERCE_NEW_ORDER";
        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                topic,
                fraudDetectorService::parse,
                Order.class)) {
            service.run();
        }
    }

    private KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("Processing new order, checking for fraud.");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        var order = record.value();
        if (isFraud(order)) {
            System.out.println("Order is a fraud: " + order);
            orderKafkaDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
        } else {
            System.out.println("Approved: " + order);
            orderKafkaDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getUserId(), order);
        }

        System.out.println("Order processed!");
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
