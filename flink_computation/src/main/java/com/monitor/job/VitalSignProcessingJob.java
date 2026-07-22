package com.monitor.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.monitor.function.*;
import com.monitor.model.*;
import com.monitor.sink.AlertEventKafkaSink;
import com.monitor.sink.DiagnosticInputKafkaSink;
import com.monitor.sink.InfluxDbSink;
import com.monitor.sink.HBaseSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Flink 实时计算主 Job — 体征数据处理全流程。
 *
 * 拓扑:
 *   Kafka Source → KeyBy(patientId) → Session Window(2s)
 *   → DeviceFusion → MEWS → Trend → Risk → Kafka Sink + InfluxDB Sink
 */
public class VitalSignProcessingJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String bootstrapServers = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String kafkaGroupId = System.getenv().getOrDefault(
            "KAFKA_GROUP_ID", "flink-computation");
        String influxUrl = System.getenv().getOrDefault(
            "INFLUXDB_URL", "http://localhost:8086");
        String influxToken = System.getenv().getOrDefault(
            "INFLUXDB_TOKEN", "admin-token");
        String influxOrg = System.getenv().getOrDefault(
            "INFLUXDB_ORG", "hospital");
        String influxBucket = System.getenv().getOrDefault(
            "INFLUXDB_BUCKET", "vitals");

        // Source
        KafkaSource<PatientVitalRecord> source = KafkaSource.<PatientVitalRecord>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics("standardized.vitals")
            .setGroupId(kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new VitalSignDeserializer())
            .build();

        DataStream<PatientVitalRecord> sourceStream = env
            .fromSource(source, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Pipeline
        DataStream<PatientSnapshot> snapshotStream = sourceStream
            .keyBy(record -> record.getPatient() != null ? record.getPatient().getPatientId() : "unknown")
            .flatMap(new DeviceFusionFunction())
            .name("DeviceFusion")
            .uid("device-fusion");

        DataStream<MewsScore> mewsStream = snapshotStream
            .map(new MewsCalculator())
            .name("MEWS Calculator")
            .uid("mews-calculator");

        SingleOutputStreamOperator<DiagnosticInput> diagnosticStream = snapshotStream
            .connect(mewsStream)
            .keyBy(s -> s.getPatientId(), m -> m.getPatientId())
            .process(new DiagnosticOutputBuilder())
            .name("Diagnostic Output Builder")
            .uid("diagnostic-builder");

        // Sinks
        diagnosticStream
            .sinkTo(DiagnosticInputKafkaSink.create(bootstrapServers))
            .name("DiagnosticInput → Kafka")
            .uid("diagnostic-kafka-sink");

        snapshotStream
            .addSink(new InfluxDbSink<>(influxUrl, influxToken, influxOrg, influxBucket))
            .name("Snapshot → InfluxDB")
            .uid("snapshot-influx-sink");

        mewsStream
            .addSink(new InfluxDbSink<>(influxUrl, influxToken, influxOrg, influxBucket))
            .name("MEWS → InfluxDB")
            .uid("mews-influx-sink");

        // ── HBase Sink（供 L4 AI 读取） ──────────────
        snapshotStream
            .addSink(new HBaseSink<>())
            .name("Snapshot → HBase")
            .uid("snapshot-hbase-sink");

        mewsStream
            .addSink(new HBaseSink<>())
            .name("MEWS → HBase")
            .uid("mews-hbase-sink");

        diagnosticStream
            .flatMap(new AlertExtractor())
            .name("Alert Extractor")
            .uid("alert-extractor")
            .keyBy(AlertEvent::getPatientId)
            .process(new AlertDeduplicator())
            .name("Alert Deduplicator")
            .uid("alert-deduplicator")
            .sinkTo(AlertEventKafkaSink.create(bootstrapServers))
            .name("Alert → Kafka")
            .uid("alert-kafka-sink");

        // 告警也写入 HBase
        diagnosticStream
            .flatMap(new AlertExtractor())
            .name("Alert Extractor (for HBase)")
            .uid("alert-extractor-hbase")
            .keyBy(AlertEvent::getPatientId)
            .process(new AlertDeduplicator())
            .name("Alert Dedup (for HBase)")
            .uid("alert-dedup-hbase")
            .addSink(new HBaseSink<>())
            .name("Alert → HBase")
            .uid("alert-hbase-sink");

        env.execute("VitalSignProcessingJob");
    }

    // ── DiagnosticOutputBuilder ─────────────────────────────────
    public static class DiagnosticOutputBuilder
        extends org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction<
            String, PatientSnapshot, MewsScore, DiagnosticInput> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<MewsScore> pendingMews;
        private transient MapState<String, LinkedList<Double>> historyState;
        private transient MapState<String, BaselineStats> baselineState;
        private transient RiskClassifier riskClassifier;
        private static final int HISTORY_SIZE = 10;

        @Override
        public void open(Configuration parameters) {
            pendingMews = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pendingMews", TypeInformation.of(MewsScore.class)));

            historyState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("trendHistory",
                    TypeInformation.of(String.class),
                    TypeInformation.of(new TypeHint<LinkedList<Double>>() {})));

            baselineState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("baselineStats",
                    TypeInformation.of(String.class),
                    TypeInformation.of(BaselineStats.class)));

            riskClassifier = new RiskClassifier();
        }

        @Override
        public void processElement1(PatientSnapshot snapshot, Context ctx, Collector<DiagnosticInput> out) throws Exception {
            MewsScore mews = pendingMews.value();
            if (mews == null) {
                mews = MewsScore.builder().patientId(snapshot.getPatientId())
                    .timestamp(snapshot.getTimestamp()).totalScore(0)
                    .components(new HashMap<>()).riskLevel("STABLE").build();
            }
            buildAndEmit(snapshot, mews, out);
        }

        @Override
        public void processElement2(MewsScore mews, Context ctx, Collector<DiagnosticInput> out) throws Exception {
            pendingMews.update(mews);
        }

        private void buildAndEmit(PatientSnapshot snapshot, MewsScore mews, Collector<DiagnosticInput> out) throws Exception {
            // 趋势分析（内联，使用本类的 Flink 状态）
            Map<String, Double> currentValues = new HashMap<>();
            if (snapshot.getVitals() != null) {
                for (var e : snapshot.getVitals().entrySet()) {
                    currentValues.put(e.getKey(), e.getValue().getValue());
                }
            }

            Map<String, String> trendDirections = new HashMap<>();
            Map<String, Double> changeRates = new HashMap<>();
            List<TrendResult.AnomalyInfo> trendAnomalies = new ArrayList<>();

            for (Map.Entry<String, Double> entry : currentValues.entrySet()) {
                String param = entry.getKey();
                double value = entry.getValue();

                LinkedList<Double> history = historyState.get(param);
                if (history == null) history = new LinkedList<>();
                history.addLast(value);
                if (history.size() > HISTORY_SIZE) history.removeFirst();
                historyState.put(param, history);

                if (history.size() >= 3) {
                    double avgFirst = (history.get(0) + history.get(1)) / 2.0;
                    double avgLast = (history.get(history.size() - 2) + history.getLast()) / 2.0;
                    double changePct = avgFirst > 0 ? Math.round(((avgLast - avgFirst) / avgFirst) * 100 * 10.0) / 10.0 : 0;
                    changeRates.put(param, changePct);

                    String trend = Math.abs(changePct) > 15 ? "rapid_change"
                        : changePct > 5 ? "rising" : changePct < -5 ? "falling" : "stable";
                    trendDirections.put(param, trend);

                    if ("rapid_change".equals(trend)) {
                        trendAnomalies.add(TrendResult.AnomalyInfo.builder()
                            .parameter(param).type("rapid_change").severity("WARNING")
                            .description(String.format("%s 在 %d 个周期内变化 %.1f%%", param, HISTORY_SIZE, changePct))
                            .build());
                    }
                }

                BaselineStats baseline = baselineState.get(param);
                if (baseline == null) {
                    baseline = BaselineStats.builder().mean(value).stddev(0).count(1).build();
                } else {
                    double newMean = (baseline.getMean() * baseline.getCount() + value) / (baseline.getCount() + 1);
                    double newStddev = Math.sqrt(
                        (baseline.getStddev() * baseline.getStddev() * baseline.getCount()
                            + (value - newMean) * (value - newMean)) / (baseline.getCount() + 1));
                    baseline = BaselineStats.builder()
                        .mean(Math.round(newMean * 10.0) / 10.0)
                        .stddev(Math.round(newStddev * 10.0) / 10.0)
                        .count(baseline.getCount() + 1).build();
                }
                baselineState.put(param, baseline);

                if (baseline.getStddev() > 0.5 && baseline.getCount() > 5) {
                    double deviation = Math.abs(value - baseline.getMean()) / Math.max(baseline.getStddev(), 0.01);
                    if (deviation > 3.0) {
                        trendAnomalies.add(TrendResult.AnomalyInfo.builder()
                            .parameter(param).type("baseline_deviation").severity("CRITICAL")
                            .description(String.format("%s=%.1f 偏离基线(%.1f±%.1f) %.1fσ",
                                param, value, baseline.getMean(), baseline.getStddev(), deviation))
                            .build());
                    }
                }
            }

            TrendResult trend = TrendResult.builder()
                .patientId(snapshot.getPatientId()).timestamp(snapshot.getTimestamp())
                .mewsScore(mews).trendDirections(trendDirections).changeRates(changeRates)
                .anomalies(trendAnomalies).build();

            String riskLevel = riskClassifier.classify(mews, trend);

            // 构建 vitals map
            Map<String, DiagnosticInput.VitalSignInfo> vitalsInfo = new HashMap<>();
            if (snapshot.getVitals() != null) {
                for (var e : snapshot.getVitals().entrySet()) {
                    String param = e.getKey();
                    PatientSnapshot.VitalSign vs = e.getValue();
                    String ts = trendDirections.getOrDefault(param, "stable");
                    double cr = changeRates.getOrDefault(param, 0.0);
                    vitalsInfo.put(param, DiagnosticInput.VitalSignInfo.builder()
                        .value(vs.getValue()).unit(vs.getUnit()).trend(ts)
                        .changeRate(cr).isAnomalous("rapid_change".equals(ts)).build());
                }
            }

            // 构建 anomalies
            List<DiagnosticInput.AnomalyInfo> outputAnomalies = new ArrayList<>();
            if (trend.getAnomalies() != null) {
                for (var a : trend.getAnomalies()) {
                    Map<String, Object> details = new HashMap<>();
                    details.put("changeRate", changeRates.getOrDefault(a.getParameter(), 0.0));
                    outputAnomalies.add(DiagnosticInput.AnomalyInfo.builder()
                        .parameter(a.getParameter()).type(a.getType()).severity(a.getSeverity())
                        .description(a.getDescription()).details(details).build());
                }
            }

            DiagnosticInput.MewsInfo mewsInfo = DiagnosticInput.MewsInfo.builder()
                .totalScore(mews.getTotalScore())
                .components(mews.getComponents() != null ? mews.getComponents() : new HashMap<>())
                .riskLevel(riskLevel).build();

            DiagnosticInput diagnostic = DiagnosticInput.builder()
                .schemaVersion("1.0")
                .messageId(UUID.randomUUID().toString())
                .traceId("flink_" + UUID.randomUUID().toString().replace("-", "").substring(0, 24))
                .patientId(snapshot.getPatientId()).timestamp(snapshot.getTimestamp())
                .windowStart(snapshot.getTimestamp()).windowEnd(snapshot.getTimestamp())
                .vitals(vitalsInfo).mews(mewsInfo).anomalies(outputAnomalies)
                .activeDevices(snapshot.getActiveDevices() != null ? snapshot.getActiveDevices() : new ArrayList<>())
                .dataQuality(DiagnosticInput.DataQualityInfo.builder()
                    .overall(snapshot.getDataQuality() != null ? snapshot.getDataQuality() : "good")
                    .signalLost("poor".equals(snapshot.getDataQuality())).artifactsDetected(false).build())
                .build();

            out.collect(diagnostic);
        }
    }

    // ── AlertExtractor ────────────────────────────────────────
    public static class AlertExtractor implements FlatMapFunction<DiagnosticInput, AlertEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(DiagnosticInput input, Collector<AlertEvent> out) {
            String riskLevel = input.getMews() != null ? input.getMews().getRiskLevel() : "STABLE";
            if ("STABLE".equals(riskLevel)) return;

            int mewsScore = input.getMews() != null ? input.getMews().getTotalScore() : 0;
            String triggerParam = "heartRate";
            double triggerValue = 0;
            double triggerThreshold = 0;
            String triggerTrend = "stable";

            if (input.getAnomalies() != null && !input.getAnomalies().isEmpty()) {
                var first = input.getAnomalies().get(0);
                triggerParam = first.getParameter();
                triggerThreshold = "CRITICAL".equals(first.getSeverity()) ? 7 : 5;
            }
            if (input.getVitals() != null && input.getVitals().containsKey(triggerParam)) {
                triggerValue = input.getVitals().get(triggerParam).getValue();
                triggerTrend = input.getVitals().get(triggerParam).getTrend();
            }

            Map<String, Double> snapshot = new HashMap<>();
            if (input.getVitals() != null) {
                for (var e : input.getVitals().entrySet()) {
                    snapshot.put(e.getKey(), e.getValue().getValue());
                }
            }

            AlertEvent alert = AlertEvent.builder()
                .alertId("alert_" + UUID.randomUUID().toString().replace("-", "").substring(0, 24))
                .traceId(input.getTraceId()).patientId(input.getPatientId()).timestamp(input.getTimestamp())
                .type(!input.getAnomalies().isEmpty() ? "MEWS_THRESHOLD" : "TREND_ABNORMAL")
                .severity(riskLevel).mewsScore(mewsScore).riskLevel(riskLevel)
                .trigger(AlertEvent.TriggerInfo.builder()
                    .primary(AlertEvent.ParameterInfo.builder()
                        .parameter(triggerParam).value(triggerValue).threshold(triggerThreshold).trend(triggerTrend).build())
                    .contributing(new ArrayList<>()).build())
                .description(String.format("患者 %s 风险等级: %s, MEWS=%d", input.getPatientId(), riskLevel, mewsScore))
                .suggestedAction("EMERGENCY".equals(riskLevel) ? "立即抢救！"
                    : "CRITICAL".equals(riskLevel) ? "立即检查！" : "密切观察。")
                .vitalSnapshot(snapshot).windowStart(input.getWindowStart()).windowEnd(input.getWindowEnd())
                .build();

            out.collect(alert);
        }
    }
}
