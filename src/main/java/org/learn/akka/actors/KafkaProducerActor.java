package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.japi.pf.ReceiveBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Created by abhiso on 7/11/16.
 */
public class KafkaProducerActor extends AbstractLoggingActor {

    /** The kafka producer */
    private Producer<String, String> producer;

    /** the constructor */
    public KafkaProducerActor() {
        receive(ReceiveBuilder
                .match(Config.class, this::init)
                .match(SendMessage.class, this::send)
                .build()
        );
    }

    private void init(Config config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    /**
     * send the message to kafka topic
     * @param t the message
     * @param <T> type T
     */
    private <T> void send(SendMessage<T> t) {
        String message = t.toString();
        CompletableFuture.supplyAsync(() -> producer.send(new ProducerRecord<>(t.topic, t.key, message)))
                .whenComplete((recordMetadataFuture, throwable) -> {});
    }

    /**
     * Config for Producer start
     */
    public static class Config {
        private String brokers;

        public Config(String brokers) {
            this.brokers = brokers;
        }
    }

    /**
     * message to be send to kafka
     * @param <T> type T
     */
    public static class SendMessage<T> {
        private String topic;
        private T message;
        private String key;

        public SendMessage(String topic, T message, String key) {
            this.topic = topic;
            this.message = message;
            this.key = key;
        }
    }
}
