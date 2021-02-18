package br.com.cesar.ecommerce;

import br.com.cesar.ecommerce.consumer.KafkaService;
import br.com.cesar.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main(String[] args) {
        var topic = "ECOMMERCE_NEW_ORDER";
        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService(FraudDetectorService.class.getSimpleName(),
                topic,
                fraudDetectorService::parse)) {
            service.run();
        }
    }

    private KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("Processing new order, checking for fraud.");
        System.out.println("Topic: " + record.topic());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        var message = record.value();
        var order = message.getPayload();
        if (isFraud(order)) {
            System.out.println("Order is a fraud: " + order);
            orderKafkaDispatcher.sendAndWait("ECOMMERCE_ORDER_REJECTED", message.getCorrelationId().continueWith(FraudDetectorService.class.getName()), order.getEmail(), order);
        } else {
            System.out.println("Approved: " + order);
            orderKafkaDispatcher.sendAndWait("ECOMMERCE_ORDER_APPROVED", message.getCorrelationId().continueWith(FraudDetectorService.class.getName()), order.getEmail(), order);
        }

        System.out.println("Order processed!");
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
