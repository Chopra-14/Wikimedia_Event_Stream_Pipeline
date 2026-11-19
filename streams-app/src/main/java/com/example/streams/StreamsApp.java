package com.example.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class StreamsApp {
    private static final Logger logger = LoggerFactory.getLogger(StreamsApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String TOPIC = System.getenv("KAFKA_TOPIC");
    private static final String CLICKHOUSE_URL = System.getenv("CLICKHOUSE_URL");
    private static final String CLICKHOUSE_USER = System.getenv("CLICKHOUSE_USER");
    private static final String CLICKHOUSE_PASSWORD = System.getenv("CLICKHOUSE_PASSWORD");

    public static void main(String[] args) {
        if (TOPIC == null || TOPIC.isEmpty()) {
            logger.error("KAFKA_TOPIC not set");
            System.exit(1);
        }
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Enable exactly-once processing
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(TOPIC);

        KStream<String, String> edits = source.filter((key, value) -> {
            try {
                JsonNode node = mapper.readTree(value);
                return "edit".equals(node.path("type").asText());
            } catch (Exception e) {
                logger.warn("Failed to parse JSON, skipping", e);
                return false;
            }
        });

        KGroupedStream<String, String> grouped = edits.selectKey((key, value) -> {
            try {
                JsonNode node = mapper.readTree(value);
                String wiki = node.path("wiki").asText();
                int ns = node.path("namespace").asInt();
                return wiki + "|" + ns;
            } catch (Exception e) {
                return "unknown|0";
            }
        }).groupByKey();

        TimeWindowedKStream<String, String> windowed = grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)));
        KTable<Windowed<String>, Long> counts = windowed.count(Materialized.as(Stores.inMemoryWindowStore("counts-store", Duration.ofMinutes(10), Duration.ofMinutes(5), false)));

        counts.toStream().foreach((windowedKey, count) -> {
            String compositeKey = windowedKey.key();
            String[] parts = compositeKey.split("\\|");
            String wiki = parts.length > 0 ? parts[0] : "unknown";
            int namespace = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            Instant start = Instant.ofEpochMilli(windowedKey.window().start());
            Instant end = Instant.ofEpochMilli(windowedKey.window().end());
            writeToClickHouse(start, end, wiki, namespace, count);
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void writeToClickHouse(Instant start, Instant end, String wiki, int namespace, long count) {
        String sql = "INSERT INTO edit_counts (window_start, window_end, wiki, namespace, edit_count) VALUES (?,?,?,?,?)";
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, start.toString().replace("T", " ").substring(0, 19)); // ClickHouse expects 'YYYY-MM-DD HH:MM:SS'
            ps.setString(2, end.toString().replace("T", " ").substring(0, 19));
            ps.setString(3, wiki);
            ps.setInt(4, namespace);
            ps.setLong(5, count);
            ps.executeUpdate();
        } catch (Exception e) {
            logger.error("Failed to write to ClickHouse", e);
        }
    }
}
