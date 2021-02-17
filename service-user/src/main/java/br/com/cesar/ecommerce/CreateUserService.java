package br.com.cesar.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
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
        var topic = "ECOMMERCE_NEW_ORDER";
        var createUserService = new CreateUserService();
        try (var service = new KafkaService(CreateUserService.class.getSimpleName(),
                topic,
                createUserService::parse)) {
            service.run();
        }
    }

    private KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("Processing new order, checking for new user.");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        var order = record.value();

        if (!exists(order.getEmail())) {
            insertNewUser(order.getEmail());
        } else {
            System.out.println("There's already a user with this email: " + order.getEmail());
        }

        System.out.println("Create new user finished!");
    }

    private void insertNewUser(String email) throws SQLException {
        String uuid = UUID.randomUUID().toString();
        var insert = connection.prepareStatement("insert into users (uuid, email) values (?, ?) ");
        insert.setString(1, uuid);
        insert.setString(2, email);
        insert.execute();
        System.out.println("User created: " + uuid + ":::" + email);
    }

    private boolean exists(String email) throws SQLException {
        var select = connection.prepareStatement("select uuid from users where email = ? limit 1");
        select.setString(1, email);
        var results = select.executeQuery();
        return results.next();
    }
}
