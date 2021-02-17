package br.com.cesar.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello World!!!");
        for (var i = 1; i <= 10; i++) {
            runNewOrder(String.valueOf(i));
        }
    }

    private static void runNewOrder(String index) throws InterruptedException, ExecutionException {
        var topicNewOrder = "ECOMMERCE_NEW_ORDER";
        var topicSendEmail = "ECOMMERCE_SEND_EMAIL";

        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<Email>()) {
                var emailValue = "Thank you. We are processing your order: " + index;
                var email = new Email(emailValue, emailValue);


                var orderId = UUID.randomUUID().toString();
                var amount = new BigDecimal(Math.random() * 5000 + 1);
                var randomEmail = Math.random() + "@cesar.com";
                //var randomEmail = "cesar+1@cesar.com";

                Order order = new Order(index, orderId, randomEmail, amount);

                orderDispatcher.send(topicNewOrder, new CorrelationId(NewOrderMain.class.getName()), randomEmail, order);
                emailDispatcher.send(topicSendEmail, new CorrelationId(NewOrderMain.class.getName()), randomEmail, email);
            }
        }
    }
}
