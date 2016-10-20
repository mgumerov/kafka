package test;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;

public class Server implements Runnable {
    public void run() {
        final KafkaConsumer<String, String> consumer;
        {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("enable.auto.commit", "false");
            properties.put("max.partition.fetch.bytes", "2097152");
            properties.put("group.id", UUID.randomUUID().toString());
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("client.id", "client-1");

            consumer = new KafkaConsumer<String, String>(properties);
        }
        consumer.subscribe(Arrays.asList("requests"), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            }

            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                consumer.seekToBeginning(collection.iterator().next());
            }
        });

        int timeouts = 0;
//назад по истории вернуться не получается, даже если менять имя группы.
//Видимо, нужен другой API для этого, более низкоуровневый.
//Но в принципе работает.

        KafkaProducer<String, String> producer;
        {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("enable.auto.commit", "false");
            properties.put("max.partition.fetch.bytes", "2097152");
            properties.put("group.id", "grp1.1");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("client.id", "client-1.1r");

            producer = new KafkaProducer<String, String>(properties);
        }

        //noinspection InfiniteLoopStatement
        while (true) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, String> record : records) {
                //echo msg back
                producer.send(new ProducerRecord<String, String>("responses", record.key(), record.value()));
            }
        }

    }
}
