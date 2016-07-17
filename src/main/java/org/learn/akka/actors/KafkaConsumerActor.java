package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.learn.akka.spring.SpringExtension.SpringExtProvider;

/**
 * Created by abhiso on 7/9/16.
 */
@Component("KafkaConsumerActor")
@Scope("prototype")
public class KafkaConsumerActor extends AbstractLoggingActor {

    private KafkaConsumer<String, String> consumer;

    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.consumer.topics}")
    private String topics;

    private Map<ConsumerRecord<String, String>, ActorRef> actorRefMap = new HashMap<>();
    private AtomicInteger count;
    private int messageCount;

    /**
     * constructor for the actor
     */
    public KafkaConsumerActor() {
        receive(ReceiveBuilder
                .match(Subscribe.class, this::subscribe)
                .match(Poll.class, this::poll)
                .match(Done.class, this::messageHandled)
                .match(RecoverableError.class, this::handleRecoverableError)
                .build()
        );
    }

    /**
     * initialize the kafka consumer and subscribe to the topics it's gonna listen to
     */
    private void subscribe(Subscribe subscribe) {
        log().info("Starting consumer");

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "async-test");
        props.put("offset.storage", "kafka");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topics.split(",")), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    doCommitSync();
                }
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            });
            context().sender().tell(new SubscriptionSuccess(), self());
        } catch (Exception e) {
            context().sender().tell(new SubscriptionFailure(), self());
        }
    }

    /**
     * start kafka polling
     * @param poll poll message
     */
    private void poll(Poll poll) {
        log().info("Got Poll message");
        ConsumerRecords<String, String> records = consumer.poll(1000);
        log().info("Records count " + records.count());

        if (records.count() > 0) {
            count = new AtomicInteger(0);
            messageCount = records.count();
            records.forEach(record -> {
                ActorRef handler = context().actorOf(SpringExtProvider.get(context().system()).props("MessageHandler"));
                actorRefMap.put(record, handler);
                handler.tell(new MessageHandlerActor.ProcessMessage(record), self());
            });
        } else {
            self().tell(new Poll(), self());
        }
    }

    /**
     * message has been processed
     * @param done done message from the handler
     */
    private void messageHandled(Done done) {
//        log().info("Message has been processed - " + done.consumerRecord);
        int currentCount = count.incrementAndGet();
        if (currentCount == messageCount) {
            self().tell(new Poll(), self());
        }
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
     * Subscribe Kafka Consumer to the brokers for the topics
     */
    public static class Subscribe {}

    /**
     * Start Polling message for the Kafka Consumer Actor
     */
    public static class Poll {}

    /**
     * Kafka has successfully subscribed to the broker
     * and ready to poll on messages
     */
    public static class SubscriptionSuccess {}

    /**
     * Some error came while subscribing to the topic
     */
    public static class SubscriptionFailure {}


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
