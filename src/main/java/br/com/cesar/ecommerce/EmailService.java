package br.com.cesar.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {
        var topic = "ECOMMERCE_SEND_EMAIL";
        var emailService = new EmailService();

        try (var service = new KafkaService(EmailService.class.getSimpleName(),
                topic,
                emailService::parse,
                String.class)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("Sending email, checking for fraud.");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Email sent.");
    }
}
