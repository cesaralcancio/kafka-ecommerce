package br.com.cesar.ecommerce.consumer;

import java.util.concurrent.Executors;

public class ServiceRunner<T> {

    private final ServiceProvider<T> provider;

    public ServiceRunner(ServiceFactory<T> factory) {
        provider = new ServiceProvider(factory);
    }

    public void start(int threads) {
        var pool = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(provider);
        }
    }
}
