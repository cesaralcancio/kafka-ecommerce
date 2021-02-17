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
        var topic = "SEND_MESSAGE_TO_ALL_USERS";
        var batchSendMessageService = new BatchSendMessageService();
        try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                topic,
                batchSendMessageService::parse,
                String.class)) {
            service.run();
        }
    }

    private KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, String> record) throws SQLException {
        System.out.println("Processing new batch...");
        System.out.println("Key: " + record.key());
        System.out.println("Topic: " + record.value());
        System.out.println("Partition" + record.partition());
        System.out.println("Offset: " + record.offset());
        var topic = record.value();

        for (User user : findAll()) {
            try {
                userDispatcher.send(topic, user.getUuid(), user);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Create new user finished!");
    }

    private List<User> findAll() throws SQLException {
        var select = connection.prepareStatement("select uuid from users");
        var results = select.executeQuery();
        var users = new ArrayList<User>();
        while(results.next()) {
            users.add(new User(results.getString("uuid")));
        }
        return users;
    }
}
