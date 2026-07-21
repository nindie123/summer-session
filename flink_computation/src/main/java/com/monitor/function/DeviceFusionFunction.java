package com.monitor.function;

import com.monitor.model.PatientSnapshot;
import com.monitor.model.PatientVitalRecord;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 多设备融合函数 — 将同一患者同一时间窗口内多个设备的消息融合为 PatientSnapshot。
 *
 * 融合策略:
 *   - 同一参数多次出现，取时间戳最新的
 *   - 缺值容忍（允许某些参数缺失）
 */
public class DeviceFusionFunction
    extends ProcessWindowFunction<
        PatientVitalRecord, PatientSnapshot, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(
            String patientId,
            Context context,
            Iterable<PatientVitalRecord> records,
            Collector<PatientSnapshot> out) {

        // 参数名 → VitalSign（取最新值）
        Map<String, PatientSnapshot.VitalSign> vitalsMap = new HashMap<>();
        Set<String> activeDevices = new HashSet<>();

        for (PatientVitalRecord record : records) {
            if (record.getSource() != null) {
                activeDevices.add(record.getSource().getDeviceId());
            }

            if (record.getObservations() == null) continue;

            for (PatientVitalRecord.Observation obs : record.getObservations()) {
                String paramName = mapLoincToParam(obs.getLoincCode());
                if (paramName == null) continue;

                // 用 LOINC code 映射后的参数名作为 key
                PatientSnapshot.VitalSign current = vitalsMap.get(paramName);
                if (current == null || obs.getEffectiveTimestamp().compareTo(current.getTimestamp()) > 0) {
                    vitalsMap.put(paramName, PatientSnapshot.VitalSign.builder()
                        .value(obs.getValue())
                        .unit(obs.getUnit())
                        .deviceId(record.getSource() != null ? record.getSource().getDeviceId() : "")
                        .timestamp(obs.getEffectiveTimestamp())
                        .build());
                }
            }
        }

        // 数据质量判断
        boolean hasPoorQuality = false;
        for (PatientVitalRecord record : records) {
            if (record.getProcessing() != null
                && record.getProcessing().getDataQuality() != null
                && record.getProcessing().getDataQuality().isSignalLost()) {
                hasPoorQuality = true;
                break;
            }
        }

        String timestamp = context.window().getEnd() + "000"; // ms → ISO format approx

        PatientSnapshot snapshot = PatientSnapshot.builder()
            .patientId(patientId)
            .timestamp(new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.'000'Z")
                .format(new Date(context.window().getEnd())))
            .vitals(vitalsMap)
            .activeDevices(new ArrayList<>(activeDevices))
            .dataQuality(hasPoorQuality ? "poor" : "good")
            .build();

        out.collect(snapshot);
    }

    /**
     * LOINC 编码 → 内部参数名。
     */
    private static String mapLoincToParam(String loincCode) {
        if (loincCode == null) return null;
        return switch (loincCode) {
            case "8867-4" -> "heartRate";
            case "8480-6" -> "sysBP";
            case "8462-4" -> "diaBP";
            case "2708-6" -> "spo2";
            case "9279-1" -> "respiratoryRate";
            case "8310-5" -> "temperature";
            default -> null;
        };
    }
}
