#!/usr/bin/env python3
"""Read HBase data with clean output"""
import struct, sys, os
os.environ["PYTHONIOENCODING"] = "utf-8"

try:
    import happybase
except ImportError:
    print("Please install: pip install happybase")
    sys.exit(1)

conn = happybase.Connection('localhost', 9090)
conn.open()

def read_vitals():
    print("=" * 80)
    print("HBase vitals table - Patient Vitals + MEWS Score")
    print("=" * 80)

    table = conn.table('vitals')
    count = 0
    for key, data in table.scan():
        pid = key.decode().split('_')[0]
        info = {}

        for k, v in data.items():
            cf, col = k.decode().split(':')
            try:
                if cf == 'v':
                    val = struct.unpack('>d', v)[0]
                    info[col] = round(val, 1)
                elif cf == 'm':
                    if col in ('totalScore',):
                        info['mewsScore'] = struct.unpack('>i', v)[0]
                    elif col in ('heartRate', 'sysBP', 'respiratoryRate', 'temperature', 'avpu'):
                        info['m_' + col] = struct.unpack('>i', v)[0]
                    else:
                        info[col] = v.decode()
                elif cf == 'd':
                    info[col] = v.decode()
            except:
                pass

        hr = info.get('heartRate', '?')
        sbp = info.get('sysBP', '?')
        dbp = info.get('diaBP', '?')
        spo2 = info.get('spo2', '?')
        rr = info.get('respiratoryRate', '?')
        temp = info.get('temperature', '?')
        mews = info.get('mewsScore', info.get('totalScore', '?'))
        risk = info.get('riskLevel', '?')

        print(f"  Patient {pid:<6s}| HR={str(hr):>5s} BP={str(sbp):>5s}/{str(dbp):<5s} SpO2={str(spo2):>4s} RR={str(rr):>4s} T={str(temp):>4s} | MEWS={str(mews):>2s} {str(risk):<10s}")

        count += 1
        if count >= 50:
            break

    print(f"\n  Displayed {count} records")
    print()

def read_alerts():
    print("=" * 80)
    print("HBase alerts table - Alert Events")
    print("=" * 80)

    table = conn.table('alerts')
    count = 0
    for key, data in table.scan():
        pid = key.decode().split('_')[0]
        info = {}
        for k, v in data.items():
            cf, col = k.decode().split(':')
            try:
                info[col] = v.decode()
            except:
                info[col] = str(v)

        sev = info.get('severity', '?')
        desc = info.get('description', '?')[:50]
        print(f"  {pid:<8s} | {str(sev):<10s} | {str(desc):<50s}")
        count += 1
        if count >= 20:
            break

    print(f"\n  Displayed {count} records")
    print()

read_vitals()
read_alerts()
conn.close()
