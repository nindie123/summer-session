FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build
COPY flink_computation/pom.xml /build/
COPY flink_computation/src /build/src/

RUN mvn clean package -DskipTests -B

# ── 提交阶段 ─────────────────────────────────────
FROM flink:1.19-java17

COPY --from=builder /build/target/flink-computation-1.0.0.jar /opt/flink/jobs/flink-computation-1.0.0.jar

COPY docker/flink-submit.sh /opt/flink/jobs/submit.sh
RUN chmod +x /opt/flink/jobs/submit.sh && \
    apt-get update -qq && apt-get install -y -qq curl python3 > /dev/null 2>&1

CMD ["/opt/flink/jobs/submit.sh"]
