package com.monitor.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 诊断输入 — 输出到 ai.diagnostic.input Topic 供 L4 消费。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DiagnosticInput {
    private String schemaVersion;
    private String messageId;
    private String traceId;

    private String patientId;
    private String timestamp;
    private String windowStart;
    private String windowEnd;

    /** 体征参数 → 含趋势/变化率/异常标记 */
    private Map<String, VitalSignInfo> vitals;

    private MewsInfo mews;

    /** 异常列表 */
    private List<AnomalyInfo> anomalies;

    private List<String> activeDevices;
    private DataQualityInfo dataQuality;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VitalSignInfo {
        private double value;
        private String unit;
        private String trend;       // stable | rising | falling | rapid_change
        private double changeRate;  // 变化率 %
        private boolean isAnomalous;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MewsInfo {
        private int totalScore;
        private Map<String, Integer> components;
        private String riskLevel;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnomalyInfo {
        private String parameter;
        private String type;        // rapid_change | baseline_deviation | cross_parameter
        private String severity;    // WARNING | CRITICAL
        private String description;
        private Map<String, Object> details;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataQualityInfo {
        private String overall;
        private boolean signalLost;
        private boolean artifactsDetected;
    }
}
