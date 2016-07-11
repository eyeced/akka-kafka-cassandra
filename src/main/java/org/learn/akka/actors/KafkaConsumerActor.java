package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

/**
 * Created by abhiso on 7/9/16.
 */
public class KafkaConsumerActor extends AbstractLoggingActor {

    private KafkaConsumer<String, String> consumer;
    private List<String> topics;
    private Properties properties;
    private Map<ConsumerRecord<String, String>, ActorRef> actorRefMap = new HashMap<>();

    /**
     * hookup for akka to create the Kafka Consumer Actor
     *
     * @param properties kafka consumer properties
     * @param topics topics subscribed to
     * @return
     */
    public static Props create(Properties properties, String... topics) {
        return Props.create(KafkaConsumerActor.class, properties, topics);
    }

    /**
     * constructor for the actor
     * @param props properties for the consumer
     * @param topics topics it need to listen
     */
    public KafkaConsumerActor(Properties props, String[] topics) {
        this.topics = Arrays.asList(topics);
        this.properties = props;

        receive(ReceiveBuilder
                .match(Poll.class, this::poll)
                .match(Done.class, this::messageHandled)
                .match(RecoverableError.class, this::handleRecoverableError)
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
        log().info("Starting consumer");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                doCommitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
        });
        super.preStart();
    }

    /**
     * start kafka polling
     * @param poll poll message
     */
    private void poll(Poll poll) {
        log().info("Got Poll message");
        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
        records.forEach(record -> {
            ActorRef handler = context().actorOf(Props.create(MessageHandlerActor.class));
            actorRefMap.put(record, handler);
            handler.tell(new MessageHandlerActor.ProcessMessage(record), self());
        });
    }

    /**
     * message has been processed
     * @param done done message from the handler
     */
    private void messageHandled(Done done) {
        log().info("Message has been processed - " + done.consumerRecord);
    }

    /**
     * handle the error in here
     * @param error the error
     */
    private void handleRecoverableError(RecoverableError error) {
        log().error("Got error while processing for " + error.consumerRecord);
    }

    /**
     * call in commit sync, when the partitions are revoked
     */
    private void doCommitSync() {
        try {
            consumer.commitSync();
        } catch (WakeupException e) {
            // we're shutting down, but finish the commit first and then
            // rethrow the exception so that the main loop can exit
            doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            // the commit failed with an unrecoverable error. if there is any
            // internal state which depended on the commit, you can clean it
            // up here. otherwise it's reasonable to ignore the error and go on
            log().error("Commit failed", e);
        }
    }

    /**
     * Start Polling message for the Kafka Consumer Actor
     */
    public static class Poll {}

    /**
     * BaseMessage for messages handled by Kafka Consumer Actor
     */
    public static class BaseMessage {
        ConsumerRecord consumerRecord;
        public BaseMessage(ConsumerRecord consumerRecord) {
            this.consumerRecord = consumerRecord;
        }
    }

    /**
     * Message has been processed for offset for this topic
     * and this partition represented by the Consumer Record
     */
    public static class Done extends BaseMessage {
        public Done(ConsumerRecord consumerRecord) {
            super(consumerRecord);
        }
    }

    /**
     * A recoverable error came for the consumer record
     * means send message back to the supervisor to restart this consumer
     */
    public static class RecoverableError extends BaseMessage {
        public RecoverableError(ConsumerRecord consumerRecord) {
            super(consumerRecord);
        }
    }
}
