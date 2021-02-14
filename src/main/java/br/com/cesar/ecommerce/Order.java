package br.com.cesar.ecommerce;

import java.math.BigDecimal;

public class Order {

    private final String messageId, userId, orderId;
    private final BigDecimal amount;

    public Order(String messageId, String userId, String orderId, BigDecimal amount) {
        this.messageId = messageId;
        this.userId = userId;
        this.orderId = orderId;
        this.amount = amount;
    }
}
