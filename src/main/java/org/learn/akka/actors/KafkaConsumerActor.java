package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.japi.pf.ReceiveBuilder;

import java.util.Properties;

/**
 * Created by abhiso on 7/9/16.
 */
public class KafkaConsumerActor extends AbstractLoggingActor {

    /**
     * Start Polling message for the Kafka Consumer Actor
     */
    public static class StartPolling {
        private Properties properties;
        private String[] topics;

        public StartPolling(Properties properties, String... topics) {
            this.properties = properties;
            this.topics = topics;
        }
    }

    /**
     * constructor for the actor
     * @param props properties for the consumer
     * @param topics topics it need to listen
     */
    public KafkaConsumerActor(Properties props, String[] topics) {
        receive(ReceiveBuilder
                .match(StartPolling.class, this::startPolling)
                .build()
        );
    }

    /**
     * call in the pre start
     * subscribe to the topics for the kafka consumer
     * @throws Exception
     */
    @Override
    public void preStart() throws Exception {
        super.preStart();
    }

    /**
     * start kafka polling
     * @param startPolling
     */
    private void startPolling(StartPolling startPolling) {

    }
}
