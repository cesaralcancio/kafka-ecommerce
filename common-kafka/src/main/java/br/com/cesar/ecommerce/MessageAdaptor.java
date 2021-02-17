package br.com.cesar.ecommerce;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class MessageAdaptor implements JsonSerializer<Message>, JsonDeserializer<Message> {

    @Override
    public JsonElement serialize(Message src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject object = new JsonObject();
        object.addProperty("type", src.getPayload().getClass().getName());
        object.add("correlationId", context.serialize(src.getCorrelationId()));
        object.add("payload", context.serialize(src.getPayload()));
        return object;
    }

    @Override
    public Message deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject obj = json.getAsJsonObject();
        var type = obj.get("type").getAsString();
        var correlationId = (CorrelationId) context.deserialize(obj.get("correlationId"), CorrelationId.class);

        try {
            var payload = context.deserialize(obj.get("payload"), Class.forName(type));
            return new Message(correlationId, payload);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
