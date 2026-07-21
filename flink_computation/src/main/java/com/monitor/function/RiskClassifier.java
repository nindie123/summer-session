package com.monitor.function;

import com.monitor.model.MewsScore;
import com.monitor.model.PatientSnapshot;

import java.util.*;

/**
 * 风险分级器 — 综合 MEWS 评分和趋势分析结果，判定最终风险等级。
 *
 * 规则:
 *   MEWS 0-4 + 趋势稳定 → STABLE
 *   MEWS 0-4 + 快速变化 → WARNING
 *   MEWS 5-6 + 任意     → WARNING
 *   MEWS 7-8 + 任意     → CRITICAL
 *   MEWS ≥9             → EMERGENCY
 */
public class RiskClassifier {

    /**
     * 联合 MEWS 和趋势判定最终风险等级。
     */
    public String classify(MewsScore mews, TrendResult trend) {
        int score = mews.getTotalScore();
        boolean hasRapidChange = false;

        if (trend != null && trend.getTrendDirections() != null) {
            hasRapidChange = trend.getTrendDirections().values().stream()
                .anyMatch("rapid_change"::equals);
        }

        // 基础分级
        String baseLevel;
        if (score >= 9) {
            baseLevel = "EMERGENCY";
        } else if (score >= 7) {
            baseLevel = "CRITICAL";
        } else if (score >= 5) {
            baseLevel = "WARNING";
        } else if (hasRapidChange) {
            baseLevel = "WARNING";  // 趋势预警提升
        } else {
            baseLevel = "STABLE";
        }

        return baseLevel;
    }

    /**
     * 判断是否应该产生告警。
     */
    public boolean shouldAlert(String riskLevel) {
        return "WARNING".equals(riskLevel)
            || "CRITICAL".equals(riskLevel)
            || "EMERGENCY".equals(riskLevel);
    }
}
