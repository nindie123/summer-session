package com.monitor.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 告警事件 — 输出到 ai.alerts Topic。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertEvent {
    private String alertId;
    private String traceId;
    private String patientId;
    private String timestamp;

    private String type;       // MEWS_THRESHOLD | TREND_ABNORMAL | BASELINE_DEVIATION | SIGNAL_LOSS
    private String severity;   // WARNING | CRITICAL | EMERGENCY
    private int mewsScore;
    private String riskLevel;

    private TriggerInfo trigger;
    private String description;
    private String suggestedAction;

    private Map<String, Double> vitalSnapshot;
    private String windowStart;
    private String windowEnd;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TriggerInfo {
        private ParameterInfo primary;
        private List<ParameterInfo> contributing;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ParameterInfo {
        private String parameter;
        private double value;
        private double threshold;
        private String trend;
    }
}
