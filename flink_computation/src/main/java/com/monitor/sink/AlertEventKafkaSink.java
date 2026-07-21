package com.monitor.sink;

import com.monitor.model.AlertEvent;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * AlertEvent → Kafka ai.alerts 的 Sink。
 */
public class AlertEventKafkaSink {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static KafkaSink<AlertEvent> create(String bootstrapServers) {
        return KafkaSink.<AlertEvent>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.<AlertEvent>builder()
                    .setTopic("ai.alerts")
                    .setKeySerializationSchema(keySerializer())
                    .setValueSerializationSchema(valueSerializer())
                    .build())
            .build();
    }

    private static SerializationSchema<AlertEvent> keySerializer() {
        return element -> element.getPatientId().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static SerializationSchema<AlertEvent> valueSerializer() {
        return element -> {
            try {
                return MAPPER.writeValueAsBytes(element);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize AlertEvent", e);
            }
        };
    }
}
