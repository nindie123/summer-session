#!/usr/bin/env python3
"""Kafka → InfluxDB + HBase 桥接器"""
import asyncio, json, struct, time, os, requests
from collections import defaultdict
from datetime import datetime
import traceback

os.environ["PYTHONIOENCODING"] = "utf-8"

INFLUX_URL = os.environ.get("INFLUXDB_URL", "http://localhost:8086")
INFLUX_TOKEN = "admin-token"
INFLUX_ORG = "hospital"
INFLUX_BUCKET = "vitals"

# HBase 配置
HBASE_HOST = os.environ.get("HBASE_THRIFT_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_THRIFT_PORT", "9090"))

LOINC_MAP = {"8867-4":"heartRate","8480-6":"sysBP","8462-4":"diaBP","2708-6":"spo2","9279-1":"respiratoryRate","8310-5":"temperature"}

# HBase 客户端（延迟初始化）
_hbase_state = {"table": None}

def get_hbase():
    if _hbase_state["table"] is None:
        try:
            import happybase
            conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
            conn.open()
            _hbase_state["table"] = conn.table('vitals')
            print(f"  HBase 已连接 {HBASE_HOST}:{HBASE_PORT}", flush=True)
        except Exception as e:
            print(f"  HBase 不可用: {e}", flush=True)
            _hbase_state["table"] = False
    return _hbase_state["table"] if _hbase_state["table"] else None

def write_hbase(pid, timestamp, score, risk, vitals, comps):
    """写 HBase（存字符串，HBase shell 可直接读）"""
    table = get_hbase()
    if not table:
        return False
    try:
        ts = timestamp.replace('T', ' ').replace('Z', '')[:19] if timestamp else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        max_long = 9223372036854775807
        row_key = f"{pid}_{max_long - int(time.time()*1000)}"
        data = {}
        for k, val in vitals.items():
            if val is not None:
                data[f"v:{k}"] = f"{val:.1f}"
        data["m:totalScore"] = str(score)
        data["m:riskLevel"] = risk
        for k, val in comps.items():
            data[f"m:{k}"] = str(val)
        data["d:timestamp"] = ts
        data["d:dataQuality"] = "good"
        table.put(row_key.encode(), {k.encode(): v.encode() for k, v in data.items()})
        return True
    except Exception:
        _hbase_state["table"] = None
        return False

def calc_mews(v):
    hr=v.get("heartRate",0); sbp=v.get("sysBP",0); rr=v.get("respiratoryRate",0); temp=v.get("temperature",0)
    c={}
    c["heartRate"]=3 if hr<=40 else 2 if hr<=50 else 0 if hr<=100 else 1 if hr<=110 else 2 if hr<=129 else 3
    c["sysBP"]=3 if sbp<=70 else 2 if sbp<=80 else 1 if sbp<=100 else 0 if sbp<=199 else 2
    c["respiratoryRate"]=3 if rr<=8 else 1 if rr<=11 else 0 if rr<=20 else 1 if rr<=25 else 2 if rr<=35 else 3
    c["temperature"]=2 if temp<=35.0 else 1 if temp<=36.0 else 0 if temp<=38.0 else 1 if temp<=38.5 else 2
    c["avpu"]=0
    s=sum(c.values())
    r="STABLE" if s<5 else "WARNING" if s<7 else "CRITICAL" if s<9 else "EMERGENCY"
    return s,r,c

async def run():
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    kafka_host = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = AIOKafkaConsumer("standardized.vitals", bootstrap_servers=kafka_host,
                                auto_offset_reset="latest",
                                group_id=None)
    producer = AIOKafkaProducer(bootstrap_servers=kafka_host, acks=0)
    await consumer.start()
    await producer.start()
    print("Bridge 已连接 Kafka（消费 + 产出诊断数据）", flush=True)

    latest = {}
    count=0

    while True:
        try:
            msg = await asyncio.wait_for(consumer.getone(), timeout=5)
        except asyncio.TimeoutError:
            if count>0: print(f"[{datetime.now().strftime('%H:%M:%S')}] 已处理 {count} 条", flush=True)
            count=0
            continue

        d = json.loads(msg.value)
        pid = d.get("patient",{}).get("patientId","?")
        if pid not in latest: latest[pid]={}
        for o in d.get("observations",[]):
            p = LOINC_MAP.get(o.get("loincCode",""))
            if p: latest[pid][p]=o.get("value")

        v=latest[pid]
        if "heartRate" in v and "sysBP" in v and "respiratoryRate" in v:
            score, risk, comps = calc_mews(v)
            ts_ns = int(time.time()*1_000_000_000)

            lines=""
            for param,val in v.items():
                if val is not None: lines+=f"vitals,patientId={pid},parameter={param} value={val} {ts_ns}\n"
            lines+=f"mews,patientId={pid},riskLevel={risk} totalScore={score}i"
            for k,v2 in comps.items(): lines+=f",{k}={v2}i"
            lines+=f" {ts_ns}\n"

            try:
                r=await asyncio.to_thread(lambda: requests.post(
                    f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns",
                    headers={"Authorization":f"Token {INFLUX_TOKEN}","Content-Type":"text/plain"},data=lines))
                if r.status_code<300 and count<3:
                    hr=v.get("heartRate","?"); sbp=v.get("sysBP","?"); spo2=v.get("spo2","?")
                    print(f"  {pid}: MEWS={score} {risk} HR={hr} BP={sbp} SpO2={spo2}", flush=True)
            except Exception as e:
                if count<3: print(f"  {pid} influx: {e}", flush=True)

            # 写 HBase（字符串格式，可读）
            hb_ok = write_hbase(pid, d.get("processing",{}).get("receivedAt",""), score, risk, v, comps)
            if hb_ok and count<3:
                print(f"  {pid}: HBase ok", flush=True)

            # 写 Kafka ai.diagnostic.input（模拟 Flink 产出）
            try:
                import uuid
                ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000+0000')
                diag = {
                    "schemaVersion":"1.0","messageId":str(uuid.uuid4()),
                    "traceId":f"bridge_{uuid.uuid4().hex[:24]}",
                    "patientId":pid,"timestamp":ts,
                    "windowStart":ts,"windowEnd":ts,
                    "vitals":{k:{"value":val,"unit":"count","trend":"stable","changeRate":0.0,"anomalous":False} for k,val in v.items()},
                    "mews":{"totalScore":score,"components":comps,"riskLevel":risk},
                    "anomalies":[],
                    "activeDevices":[f"mon_{pid}"] if pid else [],
                    "dataQuality":{"overall":"good","signalLost":False,"artifactsDetected":False}
                }
                await producer.send("ai.diagnostic.input", key=pid.encode(), value=json.dumps(diag).encode())
                if count<3: print(f"  {pid}: Kafka输出 ok", flush=True)
            except Exception as e:
                if count<3: print(f"  {pid} kafka输出: {e}", flush=True)

            count+=1

    await consumer.stop()
    await producer.stop()

while True:
    try:
        asyncio.run(run())
    except Exception as e:
        print(f"桥接器崩溃: {e}", flush=True)
    import traceback; traceback.print_exc()
    print("30秒后重启...", flush=True)
    time.sleep(30)
