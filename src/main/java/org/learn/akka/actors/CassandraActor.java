package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.japi.pf.ReceiveBuilder;
import org.learn.akka.store.CassandraStore;

/**
 * Created by abhiso on 7/9/16.
 */
public class CassandraActor extends AbstractLoggingActor {

    private CassandraStore cassandraStore;

    public static class InsertOne<T> {
        private String cql;
        private Object[] args;
    }

    public CassandraActor() {
        receive(ReceiveBuilder
                .match(InsertOne.class, this::insert)
                .build()
        );
    }

    private <T> void insert(InsertOne<T> insertOne) {

    }
}
