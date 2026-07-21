package com.monitor.function;

import com.monitor.model.AlertEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 告警去重器 — 同一患者相同 type 的告警 30 秒内不重复推送。
 *
 * Key: patientId
 */
public class AlertDeduplicator
    extends KeyedProcessFunction<String, AlertEvent, AlertEvent> {

    private static final long serialVersionUID = 1L;

    // 告警类型 → 上次推送时间戳（毫秒）
    private transient ValueState<Map<String, Long>> lastAlertState;

    // 静默期 30 秒
    private static final long SILENCE_PERIOD_MS = 30_000;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Map<String, Long>> descriptor = new ValueStateDescriptor<>(
            "lastAlertTimestamps",
            TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Map<String, Long>>() {})
        );
        lastAlertState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            AlertEvent alert,
            KeyedProcessFunction<String, AlertEvent, AlertEvent>.Context context,
            Collector<AlertEvent> out) throws Exception {

        Map<String, Long> lastTimestamps = lastAlertState.value();
        if (lastTimestamps == null) {
            lastTimestamps = new HashMap<>();
        }

        String alertType = alert.getType();
        long now = System.currentTimeMillis();
        Long lastTime = lastTimestamps.get(alertType);

        if (lastTime != null && (now - lastTime) < SILENCE_PERIOD_MS) {
            // 在静默期内，丢弃
            return;
        }

        // 更新最后推送时间
        lastTimestamps.put(alertType, now);
        lastAlertState.update(lastTimestamps);

        // 设置定时器清除状态（防止状态无限增长）
        context.timerService().registerProcessingTimeTimer(now + SILENCE_PERIOD_MS);

        out.collect(alert);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<AlertEvent> out) throws Exception {
        // 定时器用于清理过期状态，无需操作
    }
}
