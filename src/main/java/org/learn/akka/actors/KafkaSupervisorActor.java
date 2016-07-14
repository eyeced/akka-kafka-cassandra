package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.springframework.context.annotation.Scope;

import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by abhiso on 7/9/16.
 */
@Named("KafkaSupervisorActor")
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
        IntStream.range(0, start.numOfConsumers)
                .forEach(i -> {
                    ActorRef worker = context().actorOf(Props.create(KafkaConsumerActor.class));
                    workers.put(i, worker);
                    worker.tell(new KafkaConsumerActor.Poll(), self());
                });
    }
}
