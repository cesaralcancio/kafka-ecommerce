package br.com.cesar.ecommerce;

import br.com.cesar.ecommerce.consumer.KafkaService;
import br.com.cesar.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {

    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<Email>();

    public static void main(String[] args) {
        var topic = "ECOMMERCE_NEW_ORDER";
        var fraudDetectorService = new EmailNewOrderService();
        try (var service = new KafkaService(EmailNewOrderService.class.getSimpleName(),
                topic,
                fraudDetectorService::parse)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        var topicSendEmail = "ECOMMERCE_SEND_EMAIL";
        var message = record.value();
        var order = message.getPayload();
        var email = order.getEmail();

        var emailValue = "Thank you. We are processing your order";
        var emailObj = new Email(emailValue, emailValue);

        emailDispatcher.sendAndWait(topicSendEmail,
                message.getCorrelationId().continueWith(EmailNewOrderService.class.getName()),
                email,
                emailObj);

        System.out.println("Email new order processed!");
    }
}
