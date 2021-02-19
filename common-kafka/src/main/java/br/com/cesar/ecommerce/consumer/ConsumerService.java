package br.com.cesar.ecommerce.consumer;

import br.com.cesar.ecommerce.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {

    String topic();

    String consumerGroup();

    void parse(ConsumerRecord<String, Message<T>> record);
}
