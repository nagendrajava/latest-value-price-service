package com.assignment.price;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriceSerializer implements Serializer<PriceRecord> {
    private static final Logger log = LoggerFactory.getLogger(PriceSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public byte[] serialize(String topic, PriceRecord data) {
        try {
            if (data == null) {
                return null;
            }
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            log.error("Error serializing PriceRecord: {}", data, e);
            throw new RuntimeException("Error serializing PriceRecord", e);
        }
    }
}
