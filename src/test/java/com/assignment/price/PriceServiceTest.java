package com.assignment.price;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class PriceServiceTest {

    private PriceService service;

    @BeforeEach
    void setUp() {
        service = new PriceService();
    }

    @Test
    void testBasicBatchCompletion() {
        String batchId = service.startBatch();

        LocalDateTime t1 = LocalDateTime.now();
        List<PriceRecord> chunk = Arrays.asList(
                new PriceRecord("INS1", t1, 100.0),
                new PriceRecord("INS2", t1, 200.0));

        service.uploadChunk(batchId, chunk);
        service.completeBatch(batchId);

        List<PriceRecord> prices = service.getLatestPrices(Arrays.asList("INS1", "INS2"));
        assertEquals(2, prices.size());
        assertEquals(100.0, prices.stream().filter(p -> p.id().equals("INS1")).findFirst().get().payload());
    }

    @Test
    void testPartialBatchDataNotVisible() {
        String batchId = service.startBatch();
        LocalDateTime t1 = LocalDateTime.now();
        service.uploadChunk(batchId, Arrays.asList(new PriceRecord("INS1", t1, 100.0)));

        // Not completed yet
        List<PriceRecord> prices = service.getLatestPrices(Arrays.asList("INS1"));
        assertTrue(prices.isEmpty(), "Consumers should not see data from incomplete batches");
    }

    @Test
    void testCancelBatch() {
        String batchId = service.startBatch();
        LocalDateTime t1 = LocalDateTime.now();
        service.uploadChunk(batchId, Arrays.asList(new PriceRecord("INS1", t1, 100.0)));

        service.cancelBatch(batchId);

        // Attempting to complete a cancelled batch should throw
        assertThrows(IllegalArgumentException.class, () -> service.completeBatch(batchId));

        List<PriceRecord> prices = service.getLatestPrices(Arrays.asList("INS1"));
        assertTrue(prices.isEmpty(), "Consumers should not see cancelled batch data");
    }

    @Test
    void testLatestValueDeterminedByAsOfAndNotCompletionOrder() {
        LocalDateTime olderTime = LocalDateTime.now().minusMinutes(5);
        LocalDateTime newerTime = LocalDateTime.now();

        // Batch 1 has NEWER time, but completes FIRST
        String b1 = service.startBatch();
        service.uploadChunk(b1, Arrays.asList(new PriceRecord("INS1", newerTime, "NEW_VAL")));
        service.completeBatch(b1);

        // Batch 2 has OLDER time, but completes SECOND
        String b2 = service.startBatch();
        service.uploadChunk(b2, Arrays.asList(new PriceRecord("INS1", olderTime, "OLD_VAL")));
        service.completeBatch(b2);

        List<PriceRecord> prices = service.getLatestPrices(Arrays.asList("INS1"));
        assertEquals(1, prices.size());
        assertEquals("NEW_VAL", prices.get(0).payload(),
                "The latest price should be determined by asOf, not completion order.");
    }

    @Test
    void testConcurrentChunkUploads() throws InterruptedException {
        String batchId = service.startBatch();
        int threadCount = 10;
        int chunksPerThread = 100;
        int recordsPerChunk = 10;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < chunksPerThread; j++) {
                        List<PriceRecord> chunk = new ArrayList<>();
                        for (int k = 0; k < recordsPerChunk; k++) {
                            chunk.add(new PriceRecord("INS_" + threadIndex + "_" + j + "_" + k, LocalDateTime.now(),
                                    "PAYLOAD"));
                        }
                        service.uploadChunk(batchId, chunk);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        service.completeBatch(batchId);

        // 10 threads * 100 chunks * 10 records = 10,000 records
        List<String> allIds = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            for (int j = 0; j < chunksPerThread; j++) {
                for (int k = 0; k < recordsPerChunk; k++) {
                    allIds.add("INS_" + i + "_" + j + "_" + k);
                }
            }
        }

        List<PriceRecord> result = service.getLatestPrices(allIds);
        assertEquals(10000, result.size(), "All concurrent chunk uploads should be successfully coalesced");
    }
}
