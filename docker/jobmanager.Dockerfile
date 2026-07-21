FROM docker-flink-job AS builder

FROM flink:1.19-java17
COPY --from=builder /opt/flink/jobs/flink-computation-1.0.0.jar /opt/flink/job.jar
