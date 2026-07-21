package com.monitor.sink;

import com.monitor.model.PatientSnapshot;
import com.monitor.model.MewsScore;
import com.monitor.model.AlertEvent;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * HBase Sink — 将数据写入 HBase 供 L4 (LLM Agent) 读取。
 *
 * 写入两个表:
 *   vitals  — 体征综合数据 (RowKey: patientId_reverseTimestamp)
 *   alerts  — 告警事件
 */
public class HBaseSink<T> extends RichSinkFunction<T> {

    private static final long serialVersionUID = 1L;

    private transient Connection connection;
    private transient Table vitalsTable;
    private transient Table alertsTable;

    private static final byte[] CF_V = Bytes.toBytes("v");  // vitals
    private static final byte[] CF_M = Bytes.toBytes("m");  // mews
    private static final byte[] CF_D = Bytes.toBytes("d");  // diagnostic
    private static final byte[] CF_A = Bytes.toBytes("a");  // alert

    @Override
    public void open(Configuration parameters) {
        String quorum = System.getenv().getOrDefault("HBASE_ZOOKEEPER_QUORUM", "localhost");
        String port = System.getenv().getOrDefault("HBASE_ZOOKEEPER_PORT", "2181");

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", port);

        try {
            connection = ConnectionFactory.createConnection(conf);
            vitalsTable = connection.getTable(TableName.valueOf("vitals"));
            alertsTable = connection.getTable(TableName.valueOf("alerts"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to HBase", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (vitalsTable != null) vitalsTable.close();
        if (alertsTable != null) alertsTable.close();
        if (connection != null) connection.close();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (value instanceof PatientSnapshot snapshot) {
            writeSnapshot(snapshot);
        } else if (value instanceof MewsScore mews) {
            writeMews(mews);
        } else if (value instanceof AlertEvent alert) {
            writeAlert(alert);
        }
    }

    private String reverseTimestamp(String timestamp) {
        // 将 ISO 时间戳转为 reverseTimestamp
        // 用于 RowKey: {patientId}_{reverseTs}
        try {
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
            java.util.Date date = sdf.parse(timestamp.substring(0, 19));
            long ts = date.getTime();
            long maxLong = Long.MAX_VALUE;
            return String.format("%020d", maxLong - ts);
        } catch (Exception e) {
            return String.format("%020d", Long.MAX_VALUE - System.currentTimeMillis());
        }
    }

    private byte[] rowKey(String patientId, String timestamp) {
        return Bytes.toBytes(patientId + "_" + reverseTimestamp(timestamp));
    }

    // ── 写体征+MEWS 综合行 ──────────────────────────
    public void writeSnapshot(PatientSnapshot snapshot) throws IOException {
        if (snapshot.getVitals() == null || snapshot.getVitals().isEmpty()) return;

        byte[] row = rowKey(snapshot.getPatientId(), snapshot.getTimestamp());
        Put put = new Put(row);

        // v 列族: 体征值
        for (Map.Entry<String, PatientSnapshot.VitalSign> entry : snapshot.getVitals().entrySet()) {
            String param = entry.getKey();
            PatientSnapshot.VitalSign vs = entry.getValue();
            put.addColumn(CF_V, Bytes.toBytes(param), Bytes.toBytes(vs.getValue()));
        }

        // d 列族: 设备列表、时间戳、质量
        put.addColumn(CF_D, Bytes.toBytes("deviceIds"),
            Bytes.toBytes(String.join(",", snapshot.getActiveDevices() != null
                ? snapshot.getActiveDevices() : java.util.Collections.emptyList())));
        put.addColumn(CF_D, Bytes.toBytes("timestamp"), Bytes.toBytes(snapshot.getTimestamp()));
        put.addColumn(CF_D, Bytes.toBytes("dataQuality"),
            Bytes.toBytes(snapshot.getDataQuality() != null ? snapshot.getDataQuality() : "good"));

        vitalsTable.put(put);
    }

    // ── 写 MEWS 评分 ────────────────────────────────
    public void writeMews(MewsScore mews) throws IOException {
        byte[] row = rowKey(mews.getPatientId(), mews.getTimestamp());
        Put put = new Put(row);

        put.addColumn(CF_M, Bytes.toBytes("totalScore"), Bytes.toBytes(mews.getTotalScore()));
        put.addColumn(CF_M, Bytes.toBytes("riskLevel"), Bytes.toBytes(mews.getRiskLevel()));

        if (mews.getComponents() != null) {
            for (Map.Entry<String, Integer> entry : mews.getComponents().entrySet()) {
                put.addColumn(CF_M, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }
        }

        vitalsTable.put(put);
    }

    // ── 写告警 ──────────────────────────────────────
    public void writeAlert(AlertEvent alert) throws IOException {
        byte[] row = rowKey(alert.getPatientId(), alert.getTimestamp());
        Put put = new Put(row);

        put.addColumn(CF_A, Bytes.toBytes("type"), Bytes.toBytes(alert.getType()));
        put.addColumn(CF_A, Bytes.toBytes("severity"), Bytes.toBytes(alert.getSeverity()));
        put.addColumn(CF_A, Bytes.toBytes("mewsScore"), Bytes.toBytes(alert.getMewsScore()));
        put.addColumn(CF_A, Bytes.toBytes("riskLevel"), Bytes.toBytes(alert.getRiskLevel()));
        put.addColumn(CF_A, Bytes.toBytes("description"), Bytes.toBytes(alert.getDescription()));
        put.addColumn(CF_A, Bytes.toBytes("suggestedAction"),
            Bytes.toBytes(alert.getSuggestedAction() != null ? alert.getSuggestedAction() : ""));
        put.addColumn(CF_A, Bytes.toBytes("timestamp"), Bytes.toBytes(alert.getTimestamp()));
        put.addColumn(CF_A, Bytes.toBytes("traceId"), Bytes.toBytes(alert.getTraceId()));

        // 触发参数
        if (alert.getTrigger() != null && alert.getTrigger().getPrimary() != null) {
            var primary = alert.getTrigger().getPrimary();
            put.addColumn(CF_A, Bytes.toBytes("triggerParam"), Bytes.toBytes(primary.getParameter()));
            put.addColumn(CF_A, Bytes.toBytes("triggerValue"), Bytes.toBytes(primary.getValue()));
        }

        // 体征快照
        if (alert.getVitalSnapshot() != null) {
            String snapshotStr = alert.getVitalSnapshot().toString();
            put.addColumn(CF_A, Bytes.toBytes("vitalSnapshot"), Bytes.toBytes(snapshotStr));
        }

        alertsTable.put(put);
    }
}
