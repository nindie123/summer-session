#!/usr/bin/env python3
"""Kafka → InfluxDB 桥接器：从 Kafka 读取体征数据，计算 MEWS，写入 InfluxDB。"""
import json, struct, time, os, requests
from collections import defaultdict
from datetime import datetime, timezone

os.environ["PYTHONIOENCODING"] = "utf-8"

KAFKA_BROKER = "localhost:9092"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "admin-token"
INFLUX_ORG = "hospital"
INFLUX_ORG_HEADER = "hospital"
INFLUX_BUCKET = "vitals"
TOPIC = "standardized.vitals"
POLL_INTERVAL = 5

def calc_mews(vitals):
    """计算 MEWS 评分"""
    hr = vitals.get("heartRate", 0)
    sbp = vitals.get("sysBP", 0)
    rr = vitals.get("respiratoryRate", 0)
    temp = vitals.get("temperature", 0)
    score = 0
    comps = {}
    comps["heartRate"] = 3 if hr <= 40 else 2 if hr <= 50 else 0 if hr <= 100 else 1 if hr <= 110 else 2 if hr <= 129 else 3
    comps["sysBP"] = 3 if sbp <= 70 else 2 if sbp <= 80 else 1 if sbp <= 100 else 0 if sbp <= 199 else 2
    comps["respiratoryRate"] = 3 if rr <= 8 else 1 if rr <= 11 else 0 if rr <= 20 else 1 if rr <= 25 else 2 if rr <= 35 else 3
    comps["temperature"] = 2 if temp <= 35.0 else 1 if temp <= 36.0 else 0 if temp <= 38.0 else 1 if temp <= 38.5 else 2
    comps["avpu"] = 0
    score = sum(comps.values())
    risk = "STABLE" if score < 5 else "WARNING" if score < 7 else "CRITICAL" if score < 9 else "EMERGENCY"
    return score, risk, comps

def write_to_influxdb(patient_id, timestamp, mews_score, risk_level, components, vitals):
    """写 InfluxDB"""
    ts_ns = int(time.time() * 1_000_000_000)

    # vitals measurement
    lines = ""
    for param, val in vitals.items():
        if val is not None:
            lines += f"vitals,patientId={patient_id},parameter={param},unit=count value={val} {ts_ns}\n"

    # mews measurement
    mews_line = f"mews,patientId={patient_id},riskLevel={risk_level}"
    mews_line += f" totalScore={mews_score}i"
    for k, v in components.items():
        mews_line += f",{k}={v}i"
    mews_line += f" {ts_ns}\n"
    lines += mews_line

    resp = requests.post(
        f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns",
        headers={"Authorization": f"Token {INFLUX_TOKEN}", "Content-Type": "text/plain"},
        data=lines,
    )
    return resp.status_code

def main():
    print("Kafka → InfluxDB 桥接器启动")
    print(f"消费 {TOPIC} → 写 InfluxDB")
    print()

    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
        consumer_timeout_ms=10000,
    )

    # 缓存每个患者的最新体征
    latest_vitals = {}

    while True:
        msg_count = 0
        for msg in consumer:
            if msg.value is None:
                continue
            d = msg.value
            patient_id = d.get("patient", {}).get("patientId", "unknown")
            obs_list = d.get("observations", [])

            if patient_id not in latest_vitals:
                latest_vitals[patient_id] = {}

            for obs in obs_list:
                code = obs.get("loincCode", "")
                value = obs.get("value")
                param_map = {
                    "8867-4": "heartRate", "8480-6": "sysBP", "8462-4": "diaBP",
                    "2708-6": "spo2", "9279-1": "respiratoryRate", "8310-5": "temperature"
                }
                param = param_map.get(code)
                if param and value is not None:
                    latest_vitals[patient_id][param] = float(value)

            # 当有足够体征时才计算 MEWS
            vitals = latest_vitals.get(patient_id, {})
            if all(k in vitals for k in ["heartRate", "sysBP", "respiratoryRate"]):
                mews_score, risk_level, components = calc_mews(vitals)
                timestamp = d.get("processing", {}).get("receivedAt", datetime.now(timezone.utc).isoformat())

                try:
                    status = write_to_influxdb(patient_id, timestamp, mews_score, risk_level, components, vitals)
                    if status in (200, 204) and msg_count < 5:
                        print(f"  {patient_id}: MEWS={mews_score} {risk_level} HR={vitals.get('heartRate','?')}", flush=True)
                    elif status >= 300:
                        print(f"  {patient_id}: InfluxDB write failed: {status}", flush=True)
                except Exception as e:
                    print(f"  {patient_id}: Error: {e}", flush=True)

                msg_count += 1

        if msg_count == 0:
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] 无新数据，等待中...", end="\r", flush=True)
        else:
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] 处理了 {msg_count} 条", flush=True)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n已停止")
