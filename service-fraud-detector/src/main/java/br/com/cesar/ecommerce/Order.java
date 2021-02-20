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

    public String getOrderId() {
        return orderId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "messageId='" + messageId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", email='" + email + '\'' +
                ", amount=" + amount +
                '}';
    }

    public String getEmail() {
        return email;
    }
}
