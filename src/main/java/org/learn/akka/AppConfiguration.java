package org.learn.akka;

import akka.actor.ActorSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * Created by abhiso on 7/13/16.
 */
@Configuration
public class AppConfiguration {

    @Autowired
    private ApplicationContext applicationContext;

    public ActorSystem actorSystem() {
        ActorSystem actorSystem = ActorSystem.create("AkkaKafkaCassandra");
        return actorSystem;
    }
}
