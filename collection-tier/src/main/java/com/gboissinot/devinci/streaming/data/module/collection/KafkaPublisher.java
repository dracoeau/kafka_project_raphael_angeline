package com.gboissinot.devinci.streaming.data.module.collection;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

class KafkaPublisher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);

    private final String topicName;
    private final Producer<String, String> producer;

    KafkaPublisher(KafkaHandlerConfig config) {
        this.producer = createProducer(config);
        this.topicName = config.getTopicName();
        registerShutdownHook(producer);
    }

    private Producer<String, String> createProducer(KafkaHandlerConfig config) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers().get(0));
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(Boolean.TRUE));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    void publish(String content) {
        logger.info(String.format("Publishing %s", content));
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, content);
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                logger.error(e.getMessage(), e);
            } else {
                logger.info("Batch record sent.");
            }
        });
    }

    private void registerShutdownHook(final Producer<String, String> producer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.debug("Closing Kafka publisher ...");
            producer.close(Duration.ofMillis(2000));
            logger.info("Kafka publisher closed.");
        }));
    }
}
