package br.com.cesar.ecommerce.consumer;

import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void> {

    private ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    @Override
    public Void call() {
        var service = factory.create();
        try (var kafkaService = new KafkaService<>(service.consumerGroup(),
                service.topic(),
                service::parse)) {
            kafkaService.run();
        }

        return null;
    }
}
