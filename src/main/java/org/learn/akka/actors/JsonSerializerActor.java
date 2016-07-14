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
     */
    public static class Deserialize {
        private String json;
        private Class clazz;

        public Deserialize(String json, Class clazz) {
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
                .match(Deserialize.class, deserialize -> deserialize((Deserialize) deserialize))
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
    private <T> void serialize(Serialize<T> serializeThis) {
        try {
            objectMapper.writeValueAsString(serializeThis.value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * deserialize json to T type object
     *
     * @param deserialize deserialize object
     * @return T object
     */
    private void deserialize(Deserialize deserialize) {
        try {
            objectMapper.readValue(deserialize.json, deserialize.clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
