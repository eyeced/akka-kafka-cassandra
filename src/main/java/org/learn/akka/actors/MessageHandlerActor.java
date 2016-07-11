package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.japi.pf.ReceiveBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by abhiso on 7/10/16.
 */
public class MessageHandlerActor extends AbstractLoggingActor {

    /**
     * process the given message
     */
    public static class ProcessMessage {
        ConsumerRecord<String, String> consumerRecord;

        public ProcessMessage(ConsumerRecord<String, String> consumerRecord) {
            this.consumerRecord = consumerRecord;
        }
    }

    /**
     * constructor
     */
    public MessageHandlerActor() {
        receive(ReceiveBuilder
                .match(ProcessMessage.class, this::handle)
                .build()
        );
    }

    /**
     * handle the incoming message
     * @param processMessage process message
     */
    private void handle(ProcessMessage processMessage) {
        try {
            log().info("Got message " + processMessage.consumerRecord.value());
            context().parent().tell(new KafkaConsumerActor.Done(processMessage.consumerRecord), self());
        } catch (Exception e) {
            context().parent().tell(new KafkaConsumerActor.RecoverableError(processMessage.consumerRecord), self());
        }
    }
}
