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

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<Email>();

    @Override
    public void destroy() {
        this.orderDispatcher.close();
        this.emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        var topicNewOrder = "ECOMMERCE_NEW_ORDER";
        var topicSendEmail = "ECOMMERCE_SEND_EMAIL";

        var orderId = UUID.randomUUID().toString();
        var reqEmail = String.valueOf(req.getParameter("email"));
        var reqAmount = String.valueOf(req.getParameter("amount"));

        var randomEmail = isNullOrEmpty(reqEmail) ?
                Math.random() + "@cesar.com" : reqEmail;
        var amount = isNullOrEmpty(reqAmount) ?
                new BigDecimal(Math.random() * 5000 + 1) : new BigDecimal(reqAmount);

        var emailValue = "Thank you. We are processing your order";
        var email = new Email(emailValue, emailValue);

        Order order = new Order("1", orderId, randomEmail, amount);

        try {
            orderDispatcher.send(topicNewOrder, randomEmail, order);
            emailDispatcher.send(topicSendEmail, randomEmail, email);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().println("New order sent: " + order.toString());
    }

    private boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty() || s.isBlank() || s.equals("null");
    }
}
