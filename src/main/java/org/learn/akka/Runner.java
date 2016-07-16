package org.learn.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.learn.akka.actors.KafkaSupervisorActor;
import org.learn.akka.spring.SpringExtension;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Created by abhiso on 7/9/16.
 */
@Component
public class Runner implements CommandLineRunner {

    @Value("${num.of.consumers}")
    private int numOfConsumers;

    @Override
    public void run(String... strings) throws Exception {

        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.scan("org.learn.akka");
        ctx.refresh();

        // get the actor system from the spring context
        ActorSystem system = ctx.getBean(ActorSystem.class);

        ActorRef kafkaSupervisor = system.actorOf(SpringExtension.SpringExtProvider
                .get(system).props("KafkaSupervisorActor"), "kafka-supervisor");

        kafkaSupervisor.tell(new KafkaSupervisorActor.Start(numOfConsumers), ActorRef.noSender());
    }
}
