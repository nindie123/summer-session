#!/usr/bin/env python3
"""Kafka -> InfluxDB + HBase bridge (stable)"""
import json, time, os, requests
from datetime import datetime

os.environ["PYTHONIOENCODING"] = "utf-8"

KAFKA_HOST = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INFLUX_URL = os.environ.get("INFLUXDB_URL", "http://localhost:8086")
INFLUX_TOKEN = "admin-token"
INFLUX_ORG = "hospital"
INFLUX_BUCKET = "vitals"
HBASE_HOST = os.environ.get("HBASE_THRIFT_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_THRIFT_PORT", "9090"))

LOINC_MAP = {"8867-4":"heartRate","8480-6":"sysBP","8462-4":"diaBP","2708-6":"spo2","9279-1":"respiratoryRate","8310-5":"temperature"}

_hbase_conn = [None, None]  # [conn, table]

def get_hbase():
    if _hbase_conn[0] is None:
        try:
            import happybase
            conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=5)
            conn.open()
            _hbase_conn[0] = conn
            _hbase_conn[1] = conn.table("vitals")
        except: pass
    return _hbase_conn[1]

def reset_hbase():
    if _hbase_conn[0]:
        try: _hbase_conn[0].close()
        except: pass
    _hbase_conn[0] = _hbase_conn[1] = None

def write_influx(pid, score, risk, comps, vitals):
    try:
        ts_ns = int(time.time() * 1_000_000_000)
        lines = ""
        for p,v in vitals.items():
            if v is not None: lines += f"vitals,patientId={pid},parameter={p} value={v} {ts_ns}\n"
        lines += f"mews,patientId={pid},riskLevel={risk} totalScore={score}i"
        for k,v in comps.items(): lines += f",{k}={v}i"
        lines += f" {ts_ns}\n"
        r = requests.post(f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns",
            headers={"Authorization":f"Token {INFLUX_TOKEN}","Content-Type":"text/plain"},data=lines,timeout=3)
        if r.status_code >= 300:
            print(f"InfluxDB写失败 HTTP {r.status_code}", flush=True)
    except Exception as e:
        print(f"InfluxDB写异常: {e}", flush=True)

def write_hbase(pid, score, risk, vitals, comps):
    table = get_hbase()
    if not table: return
    try:
        rk = f"{pid}_{9223372036854775807 - int(time.time()*1000)}"
        d = {f"v:{k}":f"{v:.1f}" for k,v in vitals.items() if v is not None}
        d["m:totalScore"] = str(score)
        d["m:riskLevel"] = risk
        for k,v in comps.items(): d[f"m:{k}"] = str(v)
        d["d:timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        d["d:dataQuality"] = "good"
        table.put(rk.encode(), {k.encode():v.encode() for k,v in d.items()})
    except:
        reset_hbase()

def main():
    from kafka import KafkaConsumer

    while True:
        try:
            print("桥接器启动", flush=True)
            c = KafkaConsumer("standardized.vitals", bootstrap_servers=KAFKA_HOST,
                auto_offset_reset="latest", group_id=None, enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None)
            print("已连接Kafka", flush=True)

            latest = {}
            total = 0
            last_print = 0
            influx_ok = 0
            influx_fail = 0

            for msg in c:
                try:
                    if msg.value is None: continue
                    d = msg.value
                    pid = d.get("patient",{}).get("patientId","?")
                    if pid not in latest: latest[pid] = {}
                    for o in d.get("observations",[]):
                        p = LOINC_MAP.get(o.get("loincCode",""))
                        if p: latest[pid][p] = o.get("value")

                    v = latest.get(pid,{})
                    if "heartRate" in v and "sysBP" in v and "respiratoryRate" in v:
                        hr = v.get("heartRate",0); sbp = v.get("sysBP",0)
                        rr = v.get("respiratoryRate",0); temp = v.get("temperature",0)
                        comps = {"heartRate":3 if hr<=40 else 2 if hr<=50 else 0 if hr<=100 else 1 if hr<=110 else 2 if hr<=129 else 3}
                        comps["sysBP"]=3 if sbp<=70 else 2 if sbp<=80 else 1 if sbp<=100 else 0 if sbp<=199 else 2
                        comps["respiratoryRate"]=3 if rr<=8 else 1 if rr<=11 else 0 if rr<=20 else 1 if rr<=25 else 2 if rr<=35 else 3
                        comps["temperature"]=2 if temp<=35.0 else 1 if temp<=36.0 else 0 if temp<=38.0 else 1 if temp<=38.5 else 2
                        comps["avpu"]=0
                        s = sum(comps.values())
                        r = "STABLE" if s<5 else "WARNING" if s<7 else "CRITICAL" if s<9 else "EMERGENCY"
                        write_influx(pid, s, r, comps, v)
                        # HBase temporary disabled (region server down)
                        # write_hbase(pid, s, r, v, comps)
                        total += 1
                        if time.time() - last_print > 5:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] {total}条 {pid}={s} {r}", flush=True)
                            last_print = time.time()
                except Exception as e:
                    print(f"消息异常: {e}", flush=True)
        except Exception as e:
            print(f"断开: {e}, 5秒后重连", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
