# Akka Implementation for Kafka and Cassandra
I have been going through my learning phases for getting accustomed to the Akka's actor model, which in my perspective one of the most simplest and kick ass design pattern, a very few traits it shows is fault tolerant built into it.
I think there are far many resources for benefits of Akka which can be covered in a single blog, so let's just skip those and I will start on how I started building this application and the thought process I went through creating this application.

## Application Overview 
Here I wanted to create an application which would poll data from Kafka and writes data into Cassandra, this feels simple enough application to start with.
Few tasks I would be doing here

* Migrating some of the suggested Kafka Consumer code suggested here [Kafka Consumer](http://docs.confluent.io/3.0.0/clients/consumer.html)
* Asynchronous writing to Cassandra

## Defining Actors
First task would be defining our Actors in our design.

* **Kafka Supervisor** would start the Kafka Consumers 
* **Kafka Consumer** poll messages from Kafka Servers
* **Cassandra** persists data into Cassandra

