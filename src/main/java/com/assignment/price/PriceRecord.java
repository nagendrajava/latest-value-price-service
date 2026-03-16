package com.assignment.price;

import java.time.LocalDateTime;

/**
 * Represents a price data record for a financial instrument.
 * A Java Record is used for simplicity, immutability, and concise syntax.
 */
public record PriceRecord(
        String id,
        LocalDateTime asOf,
        Object payload) {
}
