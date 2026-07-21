package com.monitor.function;

import com.monitor.model.PatientSnapshot;
import com.monitor.model.MewsScore;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

import java.util.*;

/**
 * 趋势分析器 — 维护患者各参数的历史值，计算趋势方向和变化率。
 *
 * 输入: PatientSnapshot + MewsScore
 * 输出: 带趋势标记的 MewsScore
 */
public class TrendAnalyzer extends RichCoMapFunction<PatientSnapshot, MewsScore, TrendResult> {

    private static final long serialVersionUID = 1L;

    // 参数名 → 最近 10 个值的有序列表（用于趋势计算）
    private transient MapState<String, LinkedList<Double>> historyState;

    // 参数名 → 基线统计
    private transient MapState<String, BaselineStats> baselineState;

    private static final int HISTORY_SIZE = 10;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, LinkedList<Double>> historyDesc = new MapStateDescriptor<>(
            "trendHistory",
            TypeInformation.of(String.class),
            TypeInformation.of(new TypeHint<LinkedList<Double>>() {})
        );
        historyState = getRuntimeContext().getMapState(historyDesc);

        MapStateDescriptor<String, BaselineStats> baselineDesc = new MapStateDescriptor<>(
            "baselineStats",
            TypeInformation.of(String.class),
            TypeInformation.of(BaselineStats.class)
        );
        baselineState = getRuntimeContext().getMapState(baselineDesc);
    }

    @Override
    public TrendResult map1(PatientSnapshot snapshot) throws Exception {
        // 仅存储历史值，实际趋势计算在 map2 中与 MewsScore 一起完成
        // 输出空结果，由 map2 生成最终 TrendResult
        return null;
    }

    @Override
    public TrendResult map2(MewsScore mews) throws Exception {
        // 趋势计算在 RiskClassifier 中完成
        // 这里返回一个包含 mews 和空趋势的中间结果
        return TrendResult.builder()
            .patientId(mews.getPatientId())
            .timestamp(mews.getTimestamp())
            .mewsScore(mews)
            .trendDirections(new HashMap<>())
            .changeRates(new HashMap<>())
            .anomalies(new ArrayList<>())
            .build();
    }

    /**
     * 更新参数历史值并计算趋势。
     */
    public TrendResult analyzeTrend(
            String patientId,
            String timestamp,
            Map<String, Double> currentValues,
            MewsScore mews) throws Exception {

        Map<String, String> trendDirections = new HashMap<>();
        Map<String, Double> changeRates = new HashMap<>();
        List<TrendResult.AnomalyInfo> anomalies = new ArrayList<>();

        for (Map.Entry<String, Double> entry : currentValues.entrySet()) {
            String param = entry.getKey();
            double value = entry.getValue();

            // 更新历史
            LinkedList<Double> history = historyState.get(param);
            if (history == null) {
                history = new LinkedList<>();
            }
            history.addLast(value);
            if (history.size() > HISTORY_SIZE) {
                history.removeFirst();
            }
            historyState.put(param, history);

            // 计算趋势方向
            if (history.size() >= 3) {
                double avgFirst = (history.get(0) + history.get(1)) / 2.0;
                double avgLast = (history.get(history.size() - 2) + history.getLast()) / 2.0;

                double changePct = 0;
                if (avgFirst > 0) {
                    changePct = ((avgLast - avgFirst) / avgFirst) * 100;
                }
                changeRates.put(param, Math.round(changePct * 10.0) / 10.0);

                String trend;
                if (Math.abs(changePct) > 15) {
                    trend = "rapid_change";
                } else if (changePct > 5) {
                    trend = "rising";
                } else if (changePct < -5) {
                    trend = "falling";
                } else {
                    trend = "stable";
                }
                trendDirections.put(param, trend);

                // 快速变化 → 异常
                if ("rapid_change".equals(trend)) {
                    anomalies.add(TrendResult.AnomalyInfo.builder()
                        .parameter(param)
                        .type("rapid_change")
                        .severity("WARNING")
                        .description(String.format("%s 在 %d 个采样周期内变化 %.1f%%",
                            param, HISTORY_SIZE, changePct))
                        .build());
                }
            }

            // 基线偏离检测
            BaselineStats baseline = baselineState.get(param);
            if (baseline == null) {
                baseline = BaselineStats.builder()
                    .mean(value)
                    .stddev(0)
                    .count(1)
                    .build();
            } else {
                // 滑动更新
                double newMean = (baseline.getMean() * baseline.getCount() + value)
                    / (baseline.getCount() + 1);
                double newStddev = Math.sqrt(
                    (baseline.getStddev() * baseline.getStddev() * baseline.getCount()
                        + (value - newMean) * (value - newMean))
                    / (baseline.getCount() + 1)
                );
                baseline = BaselineStats.builder()
                    .mean(Math.round(newMean * 10.0) / 10.0)
                    .stddev(Math.round(newStddev * 10.0) / 10.0)
                    .count(baseline.getCount() + 1)
                    .build();
            }
            baselineState.put(param, baseline);

            // 偏离基线 > 3σ 检测
            if (baseline.getStddev() > 0.5 && baseline.getCount() > 5) {
                double deviation = Math.abs(value - baseline.getMean()) / Math.max(baseline.getStddev(), 0.01);
                if (deviation > 3.0) {
                    anomalies.add(TrendResult.AnomalyInfo.builder()
                        .parameter(param)
                        .type("baseline_deviation")
                        .severity("CRITICAL")
                        .description(String.format(
                            "%s=%.1f 偏离基线(%.1f±%.1f) %.1fσ",
                            param, value, baseline.getMean(), baseline.getStddev(), deviation))
                        .build());
                }
            }
        }

        return TrendResult.builder()
            .patientId(patientId)
            .timestamp(timestamp)
            .mewsScore(mews)
            .trendDirections(trendDirections)
            .changeRates(changeRates)
            .anomalies(anomalies)
            .build();
    }
}
