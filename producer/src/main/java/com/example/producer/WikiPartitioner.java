package com.example.producer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.clients.producer.Partitioner;
import java.util.Map;

public class WikiPartitioner implements Partitioner {
    @Override
    public void configure(Map<String, ?> configs) {
        // No special config needed
    }

    @Override
    public int partition(String topic, Object keyObj, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // Key is wiki string
        String key = (String) keyObj;
        if (key == null) {
            // fallback to default partitioner behavior
            return Utils.toPositive(Utils.murmur2(valueBytes)) % cluster.partitionCountForTopic(topic);
        }
        // Compute hash of the wiki field
        byte[] bytes = key.getBytes();
        int partition = Utils.toPositive(Utils.murmur2(bytes)) % cluster.partitionCountForTopic(topic);
        return partition;
    }

    @Override
    public void close() {
        // nothing to close
    }
}
