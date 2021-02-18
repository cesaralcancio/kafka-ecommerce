package br.com.cesar.ecommerce;

import java.util.UUID;

public class CorrelationId {

    private final String id;

    public CorrelationId(String title) {
        this.id = title + ":::" + UUID.randomUUID().toString();
    }

    private CorrelationId(String id, String topic) {
        this.id = id + topic;
    }

    public CorrelationId continueWith(String title) {
        return new CorrelationId(this.id + "->" + title);
        // Example: java.lang.String:::123456(Topic:::ABC)->java.lang.Integer
    }

    public CorrelationId addTopic(String topic) {
        var id = this.id;
        var formattedTopic = "(Topic:::" + topic + ")";
        return new CorrelationId(id, formattedTopic);
        // Example: java.lang.String:::123456(Topic:::ABC)
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "CorrelationId{" +
                "id='" + id + '\'' +
                '}';
    }
}
