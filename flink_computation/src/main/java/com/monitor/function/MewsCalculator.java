package com.monitor.function;

import com.monitor.model.MewsScore;
import com.monitor.model.PatientSnapshot;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * MEWS 评分计算器。
 *
 * MEWS (Modified Early Warning Score):
 *   HR:  ≤40→3, 41-50→2, 51-100→0, 101-110→1, 111-129→2, ≥130→3
 *   SBP: ≤70→3, 71-80→2, 81-100→1, 101-199→0, ≥200→2
 *   RR:  ≤8→3, 9-11→1, 12-20→0, 21-25→1, 26-35→2, ≥36→3
 *   Temp: ≤35.0→2, 35.1-36.0→1, 36.1-38.0→0, 38.1-38.5→1, ≥38.6→2
 */
public class MewsCalculator extends RichMapFunction<PatientSnapshot, MewsScore> {

    private static final long serialVersionUID = 1L;

    @Override
    public MewsScore map(PatientSnapshot snapshot) {
        Map<String, PatientSnapshot.VitalSign> vitals = snapshot.getVitals();

        double hr = getValue(vitals, "heartRate");
        double sbp = getValue(vitals, "sysBP");
        double rr = getValue(vitals, "respiratoryRate");
        double temp = getValue(vitals, "temperature");

        int hrScore = calcHrScore(hr);
        int sbpScore = calcSbpScore(sbp);
        int rrScore = calcRrScore(rr);
        int tempScore = calcTempScore(temp);

        int total = hrScore + sbpScore + rrScore + tempScore;

        String riskLevel = classifyRisk(total);

        Map<String, Integer> components = new HashMap<>();
        components.put("heartRate", hrScore);
        components.put("sysBP", sbpScore);
        components.put("respiratoryRate", rrScore);
        components.put("temperature", tempScore);
        components.put("avpu", 0);  // 模拟器暂不实现 AVPU

        return MewsScore.builder()
            .patientId(snapshot.getPatientId())
            .timestamp(snapshot.getTimestamp())
            .totalScore(total)
            .components(components)
            .riskLevel(riskLevel)
            .build();
    }

    private double getValue(Map<String, PatientSnapshot.VitalSign> vitals, String key) {
        PatientSnapshot.VitalSign vs = vitals.get(key);
        return vs != null ? vs.getValue() : 0;
    }

    static int calcHrScore(double hr) {
        if (hr <= 40) return 3;
        if (hr <= 50) return 2;
        if (hr <= 100) return 0;
        if (hr <= 110) return 1;
        if (hr <= 129) return 2;
        return 3;
    }

    static int calcSbpScore(double sbp) {
        if (sbp <= 70) return 3;
        if (sbp <= 80) return 2;
        if (sbp <= 100) return 1;
        if (sbp <= 199) return 0;
        return 2;
    }

    static int calcRrScore(double rr) {
        if (rr <= 8) return 3;
        if (rr <= 11) return 1;
        if (rr <= 20) return 0;
        if (rr <= 25) return 1;
        if (rr <= 35) return 2;
        return 3;
    }

    static int calcTempScore(double temp) {
        if (temp <= 35.0) return 2;
        if (temp <= 36.0) return 1;
        if (temp <= 38.0) return 0;
        if (temp <= 38.5) return 1;
        return 2;
    }

    static String classifyRisk(int score) {
        if (score >= 9) return "EMERGENCY";
        if (score >= 7) return "CRITICAL";
        if (score >= 5) return "WARNING";
        return "STABLE";
    }
}
