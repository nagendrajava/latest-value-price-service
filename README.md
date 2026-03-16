# Latest-Value Price Service — Kafka Edition

A single-JVM implementation of the latest-value price service using an
**embedded Apache Kafka broker** (KRaft mode, no ZooKeeper).

---

## Architecture

```
┌──────────────────────────────── Single JVM ──────────────────────────────────┐
│                                                                               │
│   PriceService API  ─────────────────────────────────────────────────────    │
│        │  startBatch / uploadPrices / completeBatch / cancelBatch            │
│        ▼                                                                      │
│   KafkaPriceService                                                           │
│        │                                                                      │
│        │  one KafkaProducer (transactional) per open batch                   │
│        ▼                                                                      │
│   ┌─────────────────────────────────────────────┐                            │
│   │   Embedded Kafka Broker (KRaft, port=auto)  │                            │
│   │                                             │                            │
│   │   topic: prices-raw   (8 partitions)        │                            │
│   │   topic: batch-events (1 partition)         │                            │
│   └─────────────────────────────────────────────┘                            │
│        │                                                                      │
│        │  KafkaConsumer  isolation.level=read_committed                       │
│        ▼                                                                      │
│   LatestPriceStore  (AtomicReference<Map> snapshot)                          │
│        │                                                                      │
│        ▼                                                                      │
│   getLatestPrices()  ──── pure in-memory read, never touches Kafka           │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| **One transactional producer per batch** | Batches can be open concurrently; each has independent transaction state. Kafka transaction epochs are per-transactional-id, so using the batchId as the id keeps them isolated. |
| **`isolation.level=read_committed`** | The broker enforces the atomicity requirement at the protocol level — consumers never see records from an aborted or still-open transaction. |
| **Eager store update on `completeBatch`** | After commit, `KafkaPriceService.completeBatch` immediately applies the records to `LatestPriceStore` without waiting for the consumer poll cycle. This gives zero-delay reads after a complete. The background consumer still processes the committed records; `applyBatch` is idempotent so the double-apply is harmless. |
| **`AtomicReference<Map>` snapshot** | `getLatestPrices` performs a single volatile read and never blocks. Writes (batch completion) build a new immutable map and CAS it in — no locks on the read path. |
| **Within-batch deduplication in `BatchState`** | If the same instrument appears in multiple parallel chunks, we merge eagerly in memory (keeping highest `asOf`) before sending to Kafka. This reduces broker load. |
| **KRaft mode** | Removes the ZooKeeper dependency entirely. `EmbeddedKafkaBroker` formats the log directory and starts a combined broker+controller node in one step. |

---

## Running the tests

```bash
# Gradle
./gradlew test

# Maven
mvn test
```

The embedded broker starts automatically as part of `@BeforeAll`. Expect the
first run to take 20–40 s while Gradle/Maven download dependencies; subsequent
runs are 5–10 s.

---

## Project layout

```
src/
├── main/java/com/priceservice/
│   ├── api/
│   │   ├── PriceRecord.java          # Immutable domain record
│   │   └── PriceService.java         # Public API interface
│   ├── impl/
│   │   ├── KafkaPriceService.java    # Main implementation
│   │   ├── BatchState.java           # Per-batch producer + staging area
│   │   └── LatestPriceStore.java     # Lock-free in-memory snapshot store
│   └── kafka/
│       ├── EmbeddedKafkaBroker.java  # KRaft broker lifecycle
│       ├── PriceRecordSerializer.java # JSON codec
│       └── Topics.java               # Topic name constants
└── test/java/com/priceservice/
    └── KafkaPriceServiceTest.java    # 21 tests covering all requirements
```

---

## REST API

Start the application:

```bash
./gradlew bootRun
# or
mvn spring-boot:run
```

### Endpoints

#### Start a batch
```
POST /batches
→ 201 Created
  Location: /batches/batch-1
  { "batchId": "batch-1" }
```

#### Upload a chunk of prices
```
POST /batches/{id}/prices
Content-Type: application/json

{
  "prices": [
    { "id": "AAPL", "asOf": "2024-01-01T10:00:00Z", "payload": { "bid": 149.5, "ask": 150.5 } },
    { "id": "MSFT", "asOf": "2024-01-01T10:00:00Z", "payload": { "bid": 299.0, "ask": 301.0 } }
  ]
}
→ 202 Accepted
```

#### Complete a batch (atomic publish)
```
POST /batches/{id}/complete
→ 204 No Content
```

#### Cancel a batch
```
POST /batches/{id}/cancel
→ 204 No Content
```

#### Query latest prices
```
GET /prices?ids=AAPL,MSFT
→ 200 OK
{
  "prices": {
    "AAPL": { "id": "AAPL", "asOf": "2024-01-01T10:00:00Z", "payload": { ... } },
    "MSFT": { "id": "MSFT", "asOf": "2024-01-01T10:00:00Z", "payload": { ... } }
  }
}
```

### Error responses

All errors return a consistent JSON envelope:

```json
{ "status": 400, "error": "Bad Request", "message": "No open batch 'batch-99'." }
```

| Status | Cause |
|--------|-------|
| 400 | Unknown/closed batch id, blank instrument id, null asOf, empty prices list |
| 400 | Missing `?ids=` query parameter |
| 415 | Wrong Content-Type (must be `application/json`) |
| 500 | Unexpected internal error |

---

## Test layers

| Test class | What it covers | Kafka needed? |
|---|---|---|
| `KafkaPriceServiceTest` | Service logic, Kafka transactions, atomicity, concurrency | Yes (embedded) |
| `PriceControllerTest` | HTTP routing, validation, serialisation, error codes | No (mock service) |
