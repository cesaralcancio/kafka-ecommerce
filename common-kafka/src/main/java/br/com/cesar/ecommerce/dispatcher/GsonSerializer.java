package br.com.cesar.ecommerce.dispatcher;

import br.com.cesar.ecommerce.Message;
import br.com.cesar.ecommerce.MessageAdaptor;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

public class GsonSerializer<T> implements Serializer<T> {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdaptor()).create();

    @Override
    public byte[] serialize(String s, T data) {
        return gson.toJson(data).getBytes();
    }
}
