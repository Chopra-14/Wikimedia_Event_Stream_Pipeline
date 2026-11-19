package com.example.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.OkHttpClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProducerApp {
    private static final Logger logger = LoggerFactory.getLogger(ProducerApp.class);
    private static final String SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static KafkaProducer<String, String> producer;
    private static String topic;

    public static void main(String[] args) {
        topic = System.getenv("KAFKA_TOPIC");
        if (topic == null || topic.isEmpty()) {
            logger.error("KAFKA_TOPIC environment variable not set");
            System.exit(1);
        }
        // Configure Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("partitioner.class", WikiPartitioner.class.getName());
        // Optional: acks=all for reliability
        props.put("acks", "all");
        producer = new KafkaProducer<>(props);

        startEventSourceWithRetry();
    }

    private static void startEventSourceWithRetry() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Runnable startTask = new Runnable() {
            @Override
            public void run() {
                try {
                    OkHttpClient client = new OkHttpClient.Builder()
                        .readTimeout(Duration.ofMinutes(10))
                        .build();
                    EventHandler handler = new EventHandler() {
                        @Override
                        public void onOpen() {
                            logger.info("Connected to SSE stream");
                        }
                        @Override
                        public void onClosed() {
                            logger.warn("SSE stream closed, will reconnect");
                            scheduleReconnect();
                        }
                        @Override
                        public void onMessage(String event, com.launchdarkly.eventsource.MessageEvent messageEvent) {
                            String data = messageEvent.getData();
                            try {
                                JsonNode node = mapper.readTree(data);
                                if (!"edit".equals(node.path("type").asText())) {
                                    return; // skip non-edit events
                                }
                                // Produce raw JSON as value, wiki as key for partitioner (key used by custom partitioner)
                                String wiki = node.path("wiki").asText();
                                ProducerRecord<String, String> record = new ProducerRecord<>(topic, wiki, data);
                                producer.send(record, (metadata, exception) -> {
                                    if (exception != null) {
                                        logger.error("Failed to send record", exception);
                                    }
                                });
                            } catch (Exception e) {
                                logger.error("Failed to process SSE message", e);
                            }
                        }
                        @Override
                        public void onComment(String comment) {
                            // ignore
                        }
                        @Override
                        public void onError(Throwable t) {
                            logger.error("Error in SSE stream", t);
                            scheduleReconnect();
                        }
                    };
                    EventSource.Builder builder = new EventSource.Builder(handler, URI.create(SSE_URL));
                    builder.client(client);
                    EventSource es = builder.build();
                    es.start();
                } catch (Exception e) {
                    logger.error("Failed to start SSE client", e);
                    scheduleReconnect();
                }
            }
            private void scheduleReconnect() {
                // exponential backoff could be added; keep it simple: retry after 5 seconds
                scheduler.schedule(this, 5, TimeUnit.SECONDS);
            }
        };
        // start immediately
        scheduler.execute(startTask);
    }
}
