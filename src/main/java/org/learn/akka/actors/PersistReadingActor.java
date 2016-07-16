package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.japi.pf.ReceiveBuilder;
import org.learn.akka.data.Reading;
import org.learn.akka.store.CassandraStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.time.Instant;
import java.util.List;

/**
 * Created by abhiso on 7/16/16.
 */
@Component("PersistReading")
@Scope("prototype")
public class PersistReadingActor extends AbstractLoggingActor {

    @Autowired
    private CassandraStore cassandraStore;

    /**
     * insert cql to insert reading
     */
    private static final String insertCql = "insert into reads(device_id, read_time, value, flags) values(?, ?, ?, ?);";

    /**
     * the constructor
     */
    public PersistReadingActor() {
        receive(ReceiveBuilder
                .match(Insert.class, this::insert)
                .match(FetchForRange.class, this::fetchRange)
                .build()
        );
    }

    /**
     * insert data into cassandra
     * @param insert insert message
     */
    public void insert(Insert insert) {
        List<Reading> readings = insert.readings;

        Observable.from(readings)
                .flatMap(reading -> Observable.from(
                        cassandraStore.async(insertCql,
                                new Object[]{reading.getDeviceId(), reading.getReadTime(), reading.getValue(), reading.getFlag()}
                        ), Schedulers.io()))
                .subscribe(
                        rows -> {},
                        throwable -> context().sender().tell(new InsertFailed(insert), self()),
                        () -> context().sender().tell(new InsertSuccessful(insert), self())
                );

    }

    /**
     * fetch data from cassandra
     * @param fetchForRange fetch for range message
     */
    public void fetchRange(FetchForRange fetchForRange) {

    }

    /**
     * Persist Reading in database
     */
    public static class Insert {
        public List<Reading> readings;
        public MessageHandlerActor.ProcessMessage message;

        public Insert(List<Reading> readings, MessageHandlerActor.ProcessMessage message) {
            this.readings = readings;
            this.message = message;
        }
    }

    /**
     * Insert was successful nothing to return here
     */
    public static class InsertSuccessful {
        public Insert insert;

        public InsertSuccessful(Insert insert) {
            this.insert = insert;
        }
    }

    /**
     * Insert failed
     */
    public static class InsertFailed {
        Insert insert;

        public InsertFailed(Insert insert) {
            this.insert = insert;
        }
    }

    /**
     * Fetch was successful
     * return the fetched reads
     */
    public static class FetchSuccessful {
        private List<Reading> readings;

        public FetchSuccessful(List<Reading> readings) {
            this.readings = readings;
        }
    }

    /**
     * Fetch query on the readings
     */
    public static class FetchForRange {
        private Long deviceId;
        private Instant start;
        private Instant end;

        public FetchForRange(Long deviceId, Instant start, Instant end) {
            this.deviceId = deviceId;
            this.start = start;
            this.end = end;
        }
    }

}
