package com.assignment.price;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Service for keeping track of the last price for financial instruments.
 * Thread-safe:
 * - Allows concurrent chunk uploads across multiple batches and within the same
 * batch.
 * - Guarantees atomic visibility of a batch upon completion.
 * - Ensures readers see isolated states (no partial batch consumption).
 */
public class PriceService {

    // Stores actively processing batches.
    // ConcurrentMap ensures safe addition/removal of batches.
    // ConcurrentLinkedQueue ensures thread-safe parallel appends of chunks within a
    // batch.
    private final ConcurrentMap<String, Collection<PriceRecord>> activeBatches = new ConcurrentHashMap<>();

    // The single source of truth for the latest prices.
    // Guarded by 'lock' to guarantee atomic batch visibility.
    private final Map<String, PriceRecord> latestPrices = new HashMap<>();

    // ReadWriteLock for controlling concurrency between Readers (Consumers) and
    // Writers (Batch Completions).
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Starts a new batch upload process.
     * 
     * @return A unique batch identifier.
     */
    public String startBatch() {
        String batchId = UUID.randomUUID().toString();
        activeBatches.put(batchId, new ConcurrentLinkedQueue<>());
        return batchId;
    }

    /**
     * Uploads a chunk of price records for an active batch.
     * Can be called concurrently by multiple threads for the same batch.
     *
     * @param batchId The batch identifier.
     * @param chunk   A list of price records to add.
     */
    public void uploadChunk(String batchId, List<PriceRecord> chunk) {
        Collection<PriceRecord> batch = activeBatches.get(batchId);
        if (batch == null) {
            throw new IllegalArgumentException("Invalid or inactive batch ID: " + batchId);
        }
        batch.addAll(chunk);
    }

    /**
     * Completes the batch, rolling its prices into the visible dataset atomically.
     * Discards the batch if it's already removed (idempotency/safety check).
     *
     * @param batchId The batch identifier.
     */
    public void completeBatch(String batchId) {
        Collection<PriceRecord> batch = activeBatches.remove(batchId);
        if (batch == null) {
            throw new IllegalArgumentException("Invalid or inactive batch ID: " + batchId);
        }

        // Optimization: Collapse duplicates within the batch before acquiring the
        // global write lock.
        // We only care about the latest 'asOf' for each instrument within this single
        // batch.
        Map<String, PriceRecord> batchLatest = new HashMap<>();
        for (PriceRecord record : batch) {
            batchLatest.merge(record.id(), record,
                    (existing, newRecord) -> newRecord.asOf().isAfter(existing.asOf()) ? newRecord : existing);
        }

        // Apply new values to the globally visible state atomically.
        lock.writeLock().lock();
        try {
            for (PriceRecord record : batchLatest.values()) {
                PriceRecord existing = latestPrices.get(record.id());
                // Only update if there is no prior record, or if this new record is strictly
                // newer.
                if (existing == null || record.asOf().isAfter(existing.asOf())) {
                    latestPrices.put(record.id(), record);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Cancels an active batch, immediately discarding any chunks uploaded so far.
     *
     * @param batchId The batch identifier.
     */
    public void cancelBatch(String batchId) {
        activeBatches.remove(batchId);
    }

    /**
     * Retrieves the latest known prices for the provided list of instrument IDs.
     * Only returns records for instruments that have published prices; missing ones
     * are skipped.
     * All prices returned are guaranteed to be from previously completed batches,
     * without seeing partial batch data.
     *
     * @param ids List of instrument identifiers to lookup.
     * @return List of the latest price records matching the IDs.
     */
    public List<PriceRecord> getLatestPrices(List<String> ids) {
        List<PriceRecord> result = new ArrayList<>(ids.size());

        // Acquire read lock to ensure we don't read halfway through another thread's
        // completeBatch() operation.
        lock.readLock().lock();
        try {
            for (String id : ids) {
                PriceRecord record = latestPrices.get(id);
                if (record != null) {
                    result.add(record);
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        return result;
    }
}
