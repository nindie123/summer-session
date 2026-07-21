package com.monitor.sink;

import com.monitor.model.DiagnosticInput;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * DiagnosticInput → Kafka ai.diagnostic.input 的 Sink。
 */
public class DiagnosticInputKafkaSink {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static KafkaSink<DiagnosticInput> create(String bootstrapServers) {
        return KafkaSink.<DiagnosticInput>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.<DiagnosticInput>builder()
                    .setTopic("ai.diagnostic.input")
                    .setKeySerializationSchema(keySerializer())
                    .setValueSerializationSchema(valueSerializer())
                    .build())
            .build();
    }

    private static SerializationSchema<DiagnosticInput> keySerializer() {
        return element -> element.getPatientId().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static SerializationSchema<DiagnosticInput> valueSerializer() {
        return element -> {
            try {
                return MAPPER.writeValueAsBytes(element);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize DiagnosticInput", e);
            }
        };
    }
}
