package br.com.cesar.ecommerce;

import br.com.cesar.ecommerce.consumer.ConsumerService;
import br.com.cesar.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<Email> {

    public static void main(String[] args) {
        int threads = 5;
        new ServiceRunner<>(EmailService::new).start(threads);
    }

    public String consumerGroup() {
        return EmailService.class.getSimpleName();
    }

    public String topic() {
        var topic = "ECOMMERCE_SEND_EMAIL";
        return topic;
    }

    public void parse(ConsumerRecord<String, Message<Email>> record) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Email sent!");
    }
}
