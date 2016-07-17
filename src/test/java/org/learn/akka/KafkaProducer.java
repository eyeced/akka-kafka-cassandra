package org.learn.akka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.learn.akka.data.Reading;
import org.learn.akka.serialize.JsonSerializer;

import java.sql.Date;
import java.time.Instant;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Created by abhiso on 7/12/16.
 */
public class KafkaProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        IntStream.range(0, 50000)
                .forEach(i -> {
                    System.out.println("Sending message");
                    Reading reading = new Reading(Long.valueOf(i + 1), 1.0, Date.from(Instant.now()), 0l);
                    producer.send(new ProducerRecord<>("akka-test", String.valueOf(i + 1), JsonSerializer.serialize(reading)));
                });
    }
}
