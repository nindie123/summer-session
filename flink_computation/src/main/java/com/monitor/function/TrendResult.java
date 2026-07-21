package com.monitor.function;

import com.monitor.model.MewsScore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 趋势分析结果 — TrendAnalyzer 的输出。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TrendResult {
    private String patientId;
    private String timestamp;
    private MewsScore mewsScore;
    private Map<String, String> trendDirections;
    private Map<String, Double> changeRates;
    private List<AnomalyInfo> anomalies;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnomalyInfo {
        private String parameter;
        private String type;       // rapid_change | baseline_deviation
        private String severity;   // WARNING | CRITICAL
        private String description;
    }
}
