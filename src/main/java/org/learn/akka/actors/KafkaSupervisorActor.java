package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.learn.akka.spring.SpringExtension;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by abhiso on 7/9/16.
 */
@Component("KafkaSupervisorActor")
@Scope("prototype")
public class KafkaSupervisorActor extends AbstractLoggingActor {

    /**
     * Start message for starting the consumers
     */
    public static class Start {

        private int numOfConsumers;

        public Start(int numOfConsumers) {
            this.numOfConsumers = numOfConsumers;
        }
    }

    @Value("${num.of.consumers}")
    private int numOfConsumers;

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
                .match(KafkaConsumerActor.SubscriptionSuccess.class, this::startConsumerPolling)
                .build()
        );
    }

    /**
     * start the kafka consumers using the properties defined
     * @param start the start object with required fields
     */
    private void startConsumers(Start start) {
        log().info("Starting consumers");
        IntStream.range(0, start.numOfConsumers)
                .forEach(i -> {
                    ActorRef worker = context().actorOf(SpringExtension.SpringExtProvider.get(context().system()).props("KafkaConsumerActor"));
                    workers.put(i, worker);
                    worker.tell(new KafkaConsumerActor.Subscribe(), self());
                });
    }

    /**
     * consumer has successfully subscribed to the message
     * @param success success message
     */
    private void startConsumerPolling(KafkaConsumerActor.SubscriptionSuccess success) {
        sender().tell(new KafkaConsumerActor.Poll(), self());
    }
}
