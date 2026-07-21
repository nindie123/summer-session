package com.monitor.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 统一体征记录 — 对应 Kafka standardized.vitals 消息。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PatientVitalRecord {
    private String schemaVersion;
    private String messageId;
    private String traceId;

    private SourceInfo source;
    private PatientInfo patient;
    private List<Observation> observations;
    private ProcessingInfo processing;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SourceInfo {
        private String deviceId;
        private String deviceType;
        private String deviceModel;
        private String bedId;
        private String wardId;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PatientInfo {
        private String patientId;
        private String assignedBedId;
        private String mrn;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Observation {
        private String loincCode;
        private String displayName;
        private double value;
        private String unit;
        private String effectiveTimestamp;
        private String deviceTimestamp;
        private String status;
        private String bodySite;
        private String method;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProcessingInfo {
        private String receivedAt;
        private double processingLatencyMs;
        private String validationStatus;
        private DataQuality dataQuality;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataQuality {
        private String signalQuality;
        private boolean artifactsDetected;
        private boolean signalLost;
    }
}
