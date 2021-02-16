package br.com.cesar.ecommerce;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        var topicNewOrder = "ECOMMERCE_NEW_ORDER";
        var topicSendEmail = "ECOMMERCE_SEND_EMAIL";

        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<Email>()) {
                var emailValue = "Thank you. We are processing your order";
                var email = new Email(emailValue, emailValue);


                var orderId = UUID.randomUUID().toString();
                var amount = new BigDecimal(Math.random() * 5000 + 1);
                var randomEmail = Math.random() + "@cesar.com";
                //var randomEmail = "cesar+1@cesar.com";

                Order order = new Order("1", orderId, randomEmail, amount);

                try {
                    orderDispatcher.send(topicNewOrder, randomEmail, order);
                    emailDispatcher.send(topicSendEmail, randomEmail, email);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                resp.getWriter().println("New order sent: " + order.toString());
            }
        }
    }
}
