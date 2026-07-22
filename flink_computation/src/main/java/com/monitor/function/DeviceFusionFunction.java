package com.monitor.function;

import com.monitor.model.PatientSnapshot;
import com.monitor.model.PatientVitalRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 设备融合 — 直接将 PatientVitalRecord 转为 PatientSnapshot。
 */
public class DeviceFusionFunction implements FlatMapFunction<PatientVitalRecord, PatientSnapshot> {

    private static final long serialVersionUID = 4L;

    @Override
    public void flatMap(PatientVitalRecord record, Collector<PatientSnapshot> out) {
        Map<String, PatientSnapshot.VitalSign> vitals = new HashMap<>();

        if (record.getObservations() != null) {
            for (PatientVitalRecord.Observation obs : record.getObservations()) {
                String paramName = mapLoincToParam(obs.getLoincCode());
                if (paramName == null) continue;

                vitals.put(paramName, PatientSnapshot.VitalSign.builder()
                    .value(obs.getValue())
                    .unit(obs.getUnit())
                    .deviceId(record.getSource() != null ? record.getSource().getDeviceId() : "")
                    .timestamp(obs.getEffectiveTimestamp())
                    .build());
            }
        }

        List<String> devices = new ArrayList<>();
        if (record.getSource() != null) {
            devices.add(record.getSource().getDeviceId());
        }

        PatientSnapshot snapshot = PatientSnapshot.builder()
            .patientId(record.getPatient() != null ? record.getPatient().getPatientId() : "unknown")
            .timestamp(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.'000Z'").format(new Date()))
            .vitals(vitals)
            .activeDevices(devices)
            .dataQuality("good")
            .build();

        out.collect(snapshot);
    }

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
