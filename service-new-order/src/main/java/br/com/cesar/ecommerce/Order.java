package br.com.cesar.ecommerce;

import java.math.BigDecimal;

public class Order {

    private final String messageId, orderId, email;
    private final BigDecimal amount;

    public Order(String messageId, String orderId, String email, BigDecimal amount) {
        this.messageId = messageId;
        this.orderId = orderId;
        this.email = email;
        this.amount = amount;
    }
}
