package org.learn.akka;

import akka.actor.ActorSystem;
import org.learn.akka.spring.SpringExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Created by abhiso on 7/13/16.
 */
@Configuration
@PropertySource("classpath:application.properties")
public class AppConfiguration {

    @Autowired
    private ApplicationContext applicationContext;

    @Bean
    public ActorSystem actorSystem() {
        ActorSystem actorSystem = ActorSystem.create("AkkaKafkaCassandra");
        SpringExtension.SpringExtProvider.get(actorSystem).initialize(applicationContext);
        return actorSystem;
    }
}
