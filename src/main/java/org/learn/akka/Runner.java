package org.learn.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.learn.akka.actors.KafkaSupervisorActor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by abhiso on 7/9/16.
 */
@Component
public class Runner implements CommandLineRunner {

    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.topics}")
    private String topics;

    @Value("${num.of.consumers}")
    private int numOfConsumers;

    @Override
    public void run(String... strings) throws Exception {
        ActorSystem system = ActorSystem.create("kafka-cassandra");

        ActorRef kafkaSupervisor = system.actorOf(Props.create(KafkaSupervisorActor.class), "kafka-supervisor");

        kafkaSupervisor.tell(new KafkaSupervisorActor.Start(numOfConsumers, brokers, topics.split(",")), ActorRef.noSender());
    }
}
