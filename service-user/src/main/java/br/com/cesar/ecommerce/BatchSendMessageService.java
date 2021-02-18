package br.com.cesar.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        var url = "jdbc:sqlite:service-user/target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table users (" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200))");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        var topic = "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS";
        var batchSendMessageService = new BatchSendMessageService();
        try (var service = new KafkaService(BatchSendMessageService.class.getSimpleName(),
                topic,
                batchSendMessageService::parse)) {
            service.run();
        }
    }

    private KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException {
        System.out.println("Processing new batch...");
        System.out.println("Topic: " + record.topic());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        var message = record.value();
        var topic = message.getPayload();

        for (User user : findAll()) {
            try {
                userDispatcher.send(topic, message.getCorrelationId().continueWith(BatchSendMessageService.class.getSimpleName()), user.getUuid(), user);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("batch send message server finished!");
    }

    private List<User> findAll() throws SQLException {
        var select = connection.prepareStatement("select uuid from users");
        var results = select.executeQuery();
        var users = new ArrayList<User>();
        while (results.next()) {
            users.add(new User(results.getString("uuid")));
        }
        return users;
    }
}
