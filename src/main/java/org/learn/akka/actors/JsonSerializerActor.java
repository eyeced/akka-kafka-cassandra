package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.japi.pf.ReceiveBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Created by abhiso on 7/11/16.
 */
public class JsonSerializerActor extends AbstractLoggingActor {

    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Serialize object to json
     *
     * @param <T> type T
     */
    public static class Serialize<T> {
        private T value;

        public Serialize(T value) {
            this.value = value;
        }
    }

    /**
     * deserialize json to object
     *
     * @param <T> type T
     */
    public static class Deserialize<T> {
        private String json;
        private Class<T> clazz;

        public Deserialize(String json, Class<T> clazz) {
            this.json = json;
            this.clazz = clazz;
        }
    }

    /**
     * serialize error comes while serializing
     * @param <T> type T object
     */
    public static class SerializeError<T> {
        private Serialize<T> serialize;

        public SerializeError(Serialize<T> serialize) {
            this.serialize = serialize;
        }
    }

    public JsonSerializerActor() {
        receive(ReceiveBuilder
                .match(Serialize.class, this::serialize)
                .match(Deserialize.class, this::deserialize)
                .build()
        );
    }

    /**
     * serialize object to json string
     *
     * @param serializeThis object to be serialized
     * @param <T>           type T
     * @return json string
     */
    private <T> String serialize(Serialize<T> serializeThis) {
        try {
            return objectMapper.writeValueAsString(serializeThis.value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * deserialize json to T type object
     *
     * @param deserialize deserialize object
     * @param <T>         type T
     * @return T object
     */
    private <T> T deserialize(Deserialize deserialize) {
        try {
            return (T) objectMapper.readValue(deserialize.json, deserialize.clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
