package com.assignment.price;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import java.util.Scanner;

/**
 * Demonstration of Publish-Subscribe using Kafka in a single Application.
 * 
 * Pre-requisites:
 * A local Kafka broker must be running on localhost:9092.
 * You can start one using the provided docker-compose.yml.
 */
public class Application {
    private static final Logger log = LoggerFactory.getLogger(Application.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "prices-topic";
    private static final String CONSUMER_GROUP = "price-service-group-1";

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting up Kafka Publish-Subscribe Interactive Application...");

        // 1. Initialize and start the Consumer (Subscriber) in a background thread
        KafkaPriceConsumer consumer = new KafkaPriceConsumer(BOOTSTRAP_SERVERS, TOPIC, CONSUMER_GROUP);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        // Give the consumer a moment to join the group and subscribe
        Thread.sleep(3000);

        // 2. Initialize the Producer (Publisher)
        try (KafkaPriceProducer producer = new KafkaPriceProducer(BOOTSTRAP_SERVERS, TOPIC);
             Scanner scanner = new Scanner(System.in)) {
            
            System.out.println("\n========================================================");
            System.out.println(" Interactive Price Service Menu ");
            System.out.println("========================================================");
            System.out.println(" Commands:");
            System.out.println("   publish <id> <price>  -> e.g. publish AAPL 150.50");
            System.out.println("   query <id1> <id2>...  -> e.g. query AAPL MSFT");
            System.out.println("   exit                  -> Shutdown application");
            System.out.println("========================================================\n");

            while (true) {
                System.out.print("> ");
                String input = scanner.nextLine().trim();
                
                if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                    break;
                }
                
                if (input.isEmpty()) {
                    continue;
                }

                String[] parts = input.split("\\s+");
                String command = parts[0].toLowerCase();

                if (command.equals("publish")) {
                    if (parts.length != 3) {
                        System.out.println("Invalid format. Usage: publish <id> <price>");
                        continue;
                    }
                    String id = parts[1].toUpperCase();
                    String price = parts[2];
                    String payload = String.format("{\"price\": %s}", price);
                    
                    producer.publish(new PriceRecord(id, LocalDateTime.now(), payload));
                    System.out.println("Published: " + id + " at " + price);

                } else if (command.equals("query")) {
                    if (parts.length < 2) {
                        System.out.println("Invalid format. Usage: query <id> [<id2>...]");
                        continue;
                    }
                    List<String> queryIds = Arrays.asList(Arrays.copyOfRange(parts, 1, parts.length));
                    for(int i = 0; i < queryIds.size(); i++) {
                        queryIds.set(i, queryIds.get(i).toUpperCase());
                    }

                    List<PriceRecord> latestPrices = consumer.getLatestPrices(queryIds);
                    System.out.println("--- Latest Prices ---");
                    if (latestPrices.isEmpty()) {
                        System.out.println("No records found for the requested IDs.");
                    } else {
                        for (PriceRecord record : latestPrices) {
                            System.out.println("Instrument: " + record.id() + "\n  Timestamp: " + record.asOf() + "\n  Payload: " + record.payload());
                        }
                    }
                    System.out.println("---------------------");
                } else {
                    System.out.println("Unknown command: " + command);
                }
            }
        }

        System.out.println("Shutting down...");
        consumer.shutdown();
        consumerThread.join();
        log.info("Application shut down successfully.");
    }
}
