package test;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.context.request.async.DeferredResult;

import java.util.*;
import java.util.concurrent.*;

public class Client {

    //todo implement some sort of sync
    private Map<String, DeferredResult<String>> signals = new ConcurrentHashMap<String, DeferredResult<String>>();
    private KafkaProducer<String, String> producer;

    public void run() {
        {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("enable.auto.commit", "false");
            properties.put("max.partition.fetch.bytes", "2097152");
            properties.put("group.id", "grp1.1");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("client.id", "client-1.1");

            producer = new KafkaProducer<String, String>(properties);
        }

        new Thread(new ResponseReceiver()).start();
    }

    public DeferredResult<String> sendOne(final String key, final String text) {
        final DeferredResult<String> deferred = new DeferredResult<String>();
        signals.put(key, deferred);
        producer.send(new ProducerRecord<String, String>("requests", key, text));
        return deferred;
    }

    private class ResponseReceiver implements Runnable {
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
            consumer.subscribe(Arrays.asList("responses"), new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                }

                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    consumer.seekToBeginning(collection.iterator().next());
                }
            });

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(200);
                for (final ConsumerRecord<String, String> record : records) {
                    if (record.key() == null) {
                        continue; //старые записи с косяками, там я еще не писал ключи
                    }
                    final DeferredResult<String> deferred = signals.remove(record.key());
                    if (deferred != null) {
                        deferred.setResult(record.value());
                    }
                }
            }
        }
    }
}
