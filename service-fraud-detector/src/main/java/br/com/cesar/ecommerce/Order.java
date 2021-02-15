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

    public BigDecimal getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "messageId='" + messageId + '\'' +
                ", userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", amount=" + amount +
                '}';
    }

    public String getUserId() {
        return userId;
    }
}
