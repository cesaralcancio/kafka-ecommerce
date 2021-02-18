package br.com.cesar.ecommerce;

import br.com.cesar.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {
        var topic = "ECOMMERCE_SEND_EMAIL";
        var emailService = new EmailService();

        try (var service = new KafkaService(EmailService.class.getSimpleName(),
                topic,
                emailService::parse)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Email>> record) {
        System.out.println("Sending email...");
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

        System.out.println("Email sent!");
    }
}
