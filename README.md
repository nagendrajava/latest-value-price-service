# Latest Value Price Service

A Java application demonstrating a high-performance price tracker for financial instruments. It began as a robust, thread-safe in-memory batch mechanism and has been expanded to support a real-time **Publish-Subscribe architecture** using **Apache Kafka**.

## Architecture & Components

The application contains two core mechanisms for managing latest prices:

### 1. Kafka Publish-Subscribe (Real-time)
A scalable streaming approach implementing pub/sub with Kafka.
* **`KafkaPriceProducer`**: Acts as the Publisher, serializing `PriceRecord` updates and dispatching them to a single Kafka topic. It guarantees ordering per instrument using the instrument ID as the message key.
* **`KafkaPriceConsumer`**: Acts as the Subscriber. It polls the Kafka topic in a separate background thread and updates a thread-safe `ConcurrentMap` to provide atomic access to the latest price values.

### 2. In-Memory Batch Service (Legacy)
Contained within `PriceService.java`, this is a high-concurrency, strictly in-memory data store. 
* It accepts massive batched chunks uploading simultaneously across multiple threads.
* It guarantees single-transaction visibility, ensuring consumers never see "half-completed" batches.

## Prerequisites

To run the full publish-subscribe demo, you will need:
- **Java 17+**
- **Apache Maven** (`mvn`)
- **Docker Compose** (for running the local Zookeeper-less Kafka broker) 

## Quick Start (Interactive Demo)

The project includes a built-in interactive CLI application to easily demo Kafka in real-time.

1. **Start the local Apache Kafka broker:**
   The project includes a highly optimized GraalVM Kafka native image for split-second local startup.
   ```bash
   docker-compose up -d
   ```

2. **Run the Interactive CLI:**
   You can launch the CLI using the provided script file which will ensure Docker is up and launch Maven:
   ```cmd
   run.bat
   ```
   *(Alternatively, run `mvn compile exec:java "-Dexec.mainClass=com.assignment.price.Application"`)*

3. **Explore the CLI:**
   Once loaded, you can send mocked market signals and instantly query consumer datasets:
   ```text
   > publish AAPL 155.00
   Published: AAPL at 155.00
   
   > publish MSFT 310.25
   Published: MSFT at 310.25
   
   > query AAPL MSFT
   --- Latest Prices ---
   Instrument: AAPL
     Timestamp: 2026-03-15T20:04:20.976551900
     Payload: {"price": 155.00}
   Instrument: MSFT
     Timestamp: 2026-03-15T20:04:40.123456700
     Payload: {"price": 310.25}
   ---------------------
   ```

4. **Tear Down:**
   To spin down the Docker broker:
   ```bash
   docker-compose down
   ```

## Configuration

* You can configure logging verbosity (e.g. toggle to `DEBUG` to see Kafka networking layer logs) inside `src/main/resources/simplelogger.properties`.

## Testing Stack

The project relies on **JUnit 5 Jupiter** for core testing configurations targeting the thread-safety logic present in the components. Build tests can be triggered simply by:
```bash
mvn test
```
