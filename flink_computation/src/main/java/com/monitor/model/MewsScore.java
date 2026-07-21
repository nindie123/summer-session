package com.monitor.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * MEWS 评分结果。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MewsScore {
    private String patientId;
    private String timestamp;
    private int totalScore;
    private Map<String, Integer> components;
    private String riskLevel;  // STABLE | WARNING | CRITICAL | EMERGENCY
}
