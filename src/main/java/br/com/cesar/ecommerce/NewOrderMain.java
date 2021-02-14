package br.com.cesar.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello World!!!");
        for (var i = 1; i <= 10; i++) {
            runNewOrder(String.valueOf(i));
        }
    }

    private static void runNewOrder(String v) throws InterruptedException, ExecutionException {
        try (var dispatcher = new KafkaDispatcher()) {
            var key = UUID.randomUUID().toString();

            var topicNewOrder = "ECOMMERCE_NEW_ORDER";
            var topicSendEmail = "ECOMMERCE_SEND_EMAIL";

            var value = key + ",13213,65464,8797,123: " + v;
            var emailValue = "Thank you. We are processing your order: " + v;

            dispatcher.send(topicNewOrder, key, value);
            dispatcher.send(topicSendEmail, key, emailValue);
        }
    }
}
