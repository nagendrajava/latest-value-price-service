package com.assignment.price;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Publisher class responsible for sending PriceRecords to a Kafka topic.
 */
public class KafkaPriceProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaPriceProducer.class);
    
    private final KafkaProducer<String, PriceRecord> producer;
    private final String topic;

    public KafkaPriceProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PriceSerializer.class.getName());
        // Configure standard producer reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        this.producer = new KafkaProducer<>(props);
        log.info("Kafka Price Producer initialized for topic: {}", topic);
    }

    /**
     * Publishes a single price record to the topic.
     */
    public void publish(PriceRecord record) {
        if (record == null || record.id() == null) {
            log.warn("Attempted to publish null record or record with null ID");
            return;
        }

        // We use the instrument ID as the Kafka message key.
        // This ensures all updates for the same instrument go to the same partition,
        // preserving order for a given instrument.
        ProducerRecord<String, PriceRecord> producerRecord = new ProducerRecord<>(topic, record.id(), record);
        
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to publish price record for instrument {}", record.id(), exception);
            } else {
                log.debug("Published price record for {} to partition {} offset {}", 
                        record.id(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
        log.info("Kafka Price Producer closed");
    }
}
