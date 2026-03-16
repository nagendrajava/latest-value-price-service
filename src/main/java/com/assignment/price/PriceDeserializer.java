package com.assignment.price;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriceDeserializer implements Deserializer<PriceRecord> {
    private static final Logger log = LoggerFactory.getLogger(PriceDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public PriceRecord deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            return objectMapper.readValue(data, PriceRecord.class);
        } catch (Exception e) {
            log.error("Error deserializing PriceRecord", e);
            throw new RuntimeException("Error deserializing PriceRecord", e);
        }
    }
}
