package com.monitor.sink;

import com.monitor.model.PatientSnapshot;
import com.monitor.model.MewsScore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * InfluxDB Sink — 将 PatientSnapshot 和 MewsScore 写入 InfluxDB。
 *
 * 使用 InfluxDB v2 HTTP API (Line Protocol)。
 * 简化实现：直接通过 HTTP POST 写入。
 */
public class InfluxDbSink<T> extends RichSinkFunction<T> {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String token;
    private final String org;
    private final String bucket;

    private transient HttpClient httpClient;

    public InfluxDbSink(String url, String token, String org, String bucket) {
        this.url = url;
        this.token = token;
        this.org = org;
        this.bucket = bucket;
    }

    @Override
    public void open(Configuration parameters) {
        httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (value instanceof PatientSnapshot snapshot) {
            writeSnapshot(snapshot);
        } else if (value instanceof MewsScore mews) {
            writeMews(mews);
        }
    }

    private void writeSnapshot(PatientSnapshot snapshot) throws Exception {
        StringBuilder sb = new StringBuilder();
        String ts = String.valueOf(System.currentTimeMillis() * 1_000_000L); // nanosecond

        for (var entry : snapshot.getVitals().entrySet()) {
            String param = entry.getKey();
            PatientSnapshot.VitalSign vs = entry.getValue();

            // Line Protocol: measurement,tag=value field=value timestamp
            sb.append("vitals,")
              .append("patientId=").append(snapshot.getPatientId()).append(",")
              .append("parameter=").append(param).append(",")
              .append("unit=").append(vs.getUnit()).append(" ")
              .append("value=").append(vs.getValue()).append(" ")
              .append(ts)
              .append("\n");
        }

        sendToInfluxDb(sb.toString());
    }

    private void writeMews(MewsScore mews) throws Exception {
        StringBuilder sb = new StringBuilder();
        String ts = String.valueOf(System.currentTimeMillis() * 1_000_000L);

        sb.append("mews,patientId=").append(mews.getPatientId())
          .append(",riskLevel=").append(mews.getRiskLevel()).append(" ")
          .append("totalScore=").append(mews.getTotalScore()).append("i,");

        for (var entry : mews.getComponents().entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("i,");
        }
        // 去掉末尾逗号
        sb.setLength(sb.length() - 1);
        sb.append(" ").append(ts).append("\n");

        sendToInfluxDb(sb.toString());
    }

    private void sendToInfluxDb(String lineProtocol) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(String.format("%s/api/v2/write?org=%s&bucket=%s&precision=ns",
                url, org, bucket)))
            .header("Authorization", "Token " + token)
            .header("Content-Type", "text/plain; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(lineProtocol))
            .timeout(Duration.ofSeconds(5))
            .build();

        HttpResponse<Void> response = httpClient.send(request,
            HttpResponse.BodyHandlers.discarding());

        if (response.statusCode() >= 300) {
            throw new RuntimeException("InfluxDB write failed: HTTP " + response.statusCode());
        }
    }
}
