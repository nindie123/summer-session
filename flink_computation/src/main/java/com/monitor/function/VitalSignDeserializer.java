package com.monitor.function;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.monitor.model.PatientVitalRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * Kafka 消息反序列化器 — JSON → PatientVitalRecord。
 * 实现 KafkaRecordDeserializationSchema 以兼容 Flink 1.19 + KafkaSource API。
 */
public class VitalSignDeserializer
    implements KafkaRecordDeserializationSchema<PatientVitalRecord> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<PatientVitalRecord> out) throws IOException {
        byte[] value = record.value();
        if (value == null || value.length == 0) {
            return;
        }
        PatientVitalRecord result = MAPPER.readValue(value, PatientVitalRecord.class);
        out.collect(result);
    }

    @Override
    public TypeInformation<PatientVitalRecord> getProducedType() {
        return TypeInformation.of(PatientVitalRecord.class);
    }
}
