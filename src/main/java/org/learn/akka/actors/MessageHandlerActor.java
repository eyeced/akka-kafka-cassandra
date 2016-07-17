package org.learn.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.learn.akka.data.Reading;
import org.learn.akka.serialize.JsonSerializer;
import org.learn.akka.spring.SpringExtension;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * Created by abhiso on 7/10/16.
 */
@Component("MessageHandler")
@Scope("prototype")
public class MessageHandlerActor extends AbstractLoggingActor {

    /**
     * constructor
     */
    public MessageHandlerActor() {
        receive(ReceiveBuilder
                .match(ProcessMessage.class, this::handle)
                .match(PersistReadingActor.InsertSuccessful.class, this::insertSuccessful)
                .match(PersistReadingActor.InsertFailed.class, this::insertFailed)
                .build()
        );
    }

    /**
     * handle the incoming message
     * @param processMessage process message
     */
    private void handle(ProcessMessage processMessage) {
        try {
//            log().info("Got message " + processMessage.consumerRecord.value());

            Reading reading = JsonSerializer.deserialize(processMessage.consumerRecord.value(), Reading.class);

            ActorRef persistActor = context().actorOf(SpringExtension.SpringExtProvider.get(context().system()).props("PersistReading"));
            persistActor.tell(new PersistReadingActor.Insert(Arrays.asList(reading), processMessage), self());
        } catch (Exception e) {
            log().error(e.toString(), e);
            context().parent().tell(new KafkaConsumerActor.RecoverableError(processMessage.consumerRecord), self());
        }
    }

    /**
     * insert was successful
     * @param insertSuccessful success message
     */
    private void insertSuccessful(PersistReadingActor.InsertSuccessful insertSuccessful) {
        context().parent().tell(new KafkaConsumerActor.Done(insertSuccessful.insert.message.consumerRecord), self());
    }

    /**
     * insert failed
     * @param insertFailed insert failed message
     */
    private void insertFailed(PersistReadingActor.InsertFailed insertFailed) {
        // TODO: handle insert failure, if the insert was a batch message then break it down into single messages
        // TODO: and then try insert for each one of then, and which ever fails send it back to parent actor and ask it to handle it
    }

    /**
     * process the given message
     */
    public static class ProcessMessage {
        ConsumerRecord<String, String> consumerRecord;

        public ProcessMessage(ConsumerRecord<String, String> consumerRecord) {
            this.consumerRecord = consumerRecord;
        }
    }

    /**
     * Message Processed successfully
     */
    public static class Successful {
        private ProcessMessage processMessage;

        public Successful(ProcessMessage processMessage) {
            this.processMessage = processMessage;
        }
    }
}
