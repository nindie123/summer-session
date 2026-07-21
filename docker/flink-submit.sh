#!/bin/bash
# 直接使用 Flink CLI 提交作业（不经过损坏的 Web Upload API）
set -e

JOB_MANAGER_URL="jobmanager:8081"
JAR_PATH="/opt/flink/jobs/flink-computation-1.0.0.jar"

echo "[FLINK] Waiting for JobManager..."
for i in $(seq 1 30); do
    if curl -sf "http://jobmanager:8081/config" > /dev/null 2>&1; then
        echo "[FLINK] JobManager ready!"
        break
    fi
    sleep 2
done

echo "[FLINK] Submitting job via CLI..."
flink run -m "$JOB_MANAGER_URL" "$JAR_PATH" 2>&1

echo "[FLINK] Job submitted. Running. Press Ctrl+C to stop."
tail -f /dev/null
