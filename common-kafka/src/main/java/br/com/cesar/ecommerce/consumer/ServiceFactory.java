package br.com.cesar.ecommerce.consumer;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
