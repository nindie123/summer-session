package com.monitor.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 患者快照 — 多设备融合后的患者当前综合状态。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PatientSnapshot {
    private String patientId;
    private String timestamp;

    /** 参数名 → 体征值 */
    private Map<String, VitalSign> vitals;

    /** 当前活跃设备列表 */
    private List<String> activeDevices;

    /** 数据质量 */
    private String dataQuality;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VitalSign {
        private double value;
        private String unit;
        private String deviceId;
        private String timestamp;
    }
}
