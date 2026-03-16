package com.assignment.price;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Subscriber class responsible for consuming PriceRecords from a Kafka topic
 * and maintaining an internally thread-safe view of the latest prices.
 */
public class KafkaPriceConsumer implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaPriceConsumer.class);

    private final KafkaConsumer<String, PriceRecord> consumer;
    // Uses ConcurrentMap for thread-safe access to the latest prices from other threads.
    private final ConcurrentMap<String, PriceRecord> latestPrices = new ConcurrentHashMap<>();
    
    public KafkaPriceConsumer(String bootstrapServers, String topic, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PriceDeserializer.class.getName());
        // Read from the beginning if no offset is found
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
        log.info("Kafka Price Consumer initialized and subscribed to topic: {}", topic);
    }

    @Override
    public void run() {
        try {
            while (true) {
                // Poll for new messages
                ConsumerRecords<String, PriceRecord> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, PriceRecord> record : records) {
                    PriceRecord priceData = record.value();
                    if (priceData != null && priceData.id() != null) {
                        // Merge the new record into the map. 
                        // Only replace if the new record is strictly newer than the currently stored one.
                        latestPrices.merge(priceData.id(), priceData, 
                            (existing, newRecord) -> newRecord.asOf().isAfter(existing.asOf()) ? newRecord : existing);
                        
                        log.debug("Consumed and processed price record for {}", priceData.id());
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore for shutdown
        } catch (Exception e) {
            log.error("Unexpected error in Kafka consumer", e);
        } finally {
            consumer.close();
        }
    }

    /**
     * Retrieves the latest known prices for the provided list of instrument IDs.
     * Thread-safe. Can be called by other threads while the consumer loop runs.
     */
    public List<PriceRecord> getLatestPrices(List<String> ids) {
        List<PriceRecord> result = new ArrayList<>(ids.size());
        for (String id : ids) {
            PriceRecord record = latestPrices.get(id);
            if (record != null) {
                result.add(record);
            }
        }
        return result;
    }

    /**
     * Allows shutting down the consumer loop safely.
     */
    public void shutdown() {
        consumer.wakeup();
    }

    @Override
    public void close() {
        shutdown();
        log.info("Kafka Price Consumer closed");
    }
}
