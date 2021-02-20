package br.com.cesar.ecommerce;

import br.com.cesar.ecommerce.consumer.ConsumerService;
import br.com.cesar.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase database;

    CreateUserService() throws SQLException {
        this.database = new LocalDatabase("service-user/target/users_database");
        var sql = "create table users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200))";
        database.createIfNotExists(sql);
    }

    public static void main(String[] args) throws SQLException {
        new ServiceRunner<>(CreateUserService::new).start(1);
    }

    @Override
    public String topic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String consumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        var message = record.value();
        var order = message.getPayload();

        if (!exists(order.getEmail())) {
            insertNewUser(order.getEmail());
        } else {
            System.out.println("There's already a user with this email: " + order.getEmail());
        }

        System.out.println("Create new user finished!");
    }

    private void insertNewUser(String email) throws SQLException {
        String uuid = UUID.randomUUID().toString();
        database.update("insert into users (uuid, email) values (?, ?) ", uuid, email);
        System.out.println("User created: " + uuid + ":::" + email);
    }

    private boolean exists(String email) throws SQLException {
        var results = database.query("select uuid from users where email = ? limit 1", email);
        return results.next();
    }
}
