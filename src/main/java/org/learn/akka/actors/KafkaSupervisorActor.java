package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Created by abhiso on 7/9/16.
 */
public class KafkaSupervisorActor extends AbstractLoggingActor {

    /**
     * Start message for starting the consumers
     */
    public static class Start {

        private int numOfConsumers;
        private String brokers;
        private String[] topics;

        public Start(int numOfConsumers, String brokers, String... topics) {
            this.numOfConsumers = numOfConsumers;
            this.topics = topics;
            this.brokers = brokers;
        }
    }

    /**
     * workers map
     */
    Map<Integer, ActorRef> workers = new HashMap<>();

    /**
     * the constructor
     */
    KafkaSupervisorActor() {
        receive(ReceiveBuilder
                .match(Start.class, this::startConsumers)
                .build()
        );
    }

    /**
     * start the kafka consumers using the properties defined
     * @param start the start object with required fields
     */
    private void startConsumers(Start start) {
        Properties props = new Properties();
        props.put("bootstrap.servers", start.brokers);
        props.put("group.id", "perf-test");
        props.put("offset.storage", "kafka");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        IntStream.range(0, start.numOfConsumers)
                .forEach(i -> {
                    ActorRef worker = context().actorOf(KafkaConsumerActor.create(props, start.topics));
                    workers.put(i, worker);
                    worker.tell(new KafkaConsumerActor.Poll(), self());
                });
    }
}
