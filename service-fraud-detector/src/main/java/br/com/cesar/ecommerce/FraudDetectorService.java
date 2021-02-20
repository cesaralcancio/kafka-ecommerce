package br.com.cesar.ecommerce;

import br.com.cesar.ecommerce.consumer.ConsumerService;
import br.com.cesar.ecommerce.consumer.ServiceRunner;
import br.com.cesar.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {

    private KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();

    private final LocalDatabase database;

    FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("service-fraud-detector/target/fraud_database");
        var sql = "create table orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean)";
        database.createIfNotExists(sql);
    }

    public static void main(String[] args) {
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }

    @Override
    public String topic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String consumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        var message = record.value();
        var order = message.getPayload();

        if (isProcessed(order)) {
            System.out.println("Order " + order + " was already processed");
            return;
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (isFraud(order)) {
            database.update("insert into orders (uuid, is_fraud) values (?, true)", order.getOrderId());
            System.out.println("Order is a fraud: " + order);
            orderKafkaDispatcher.sendAndWait("ECOMMERCE_ORDER_REJECTED", message.getCorrelationId().continueWith(FraudDetectorService.class.getName()), order.getEmail(), order);
        } else {
            database.update("insert into orders (uuid, is_fraud) values (?, false)", order.getOrderId());
            System.out.println("Approved: " + order);
            orderKafkaDispatcher.sendAndWait("ECOMMERCE_ORDER_APPROVED", message.getCorrelationId().continueWith(FraudDetectorService.class.getName()), order.getEmail(), order);
        }

        System.out.println("Order processed!");
    }

    private boolean isProcessed(Order order) throws SQLException {
        var r = database.query("select uuid from orders where uuid = ? limit 1", order.getOrderId());
        return r.next();
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
