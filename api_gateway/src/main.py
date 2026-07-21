"""API 网关入口。"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from src.routers import vitals, mews, alerts, wards

app = FastAPI(
    title="Vital Signs Monitor API",
    description="L3 附属: 体征数据查询服务 (供 L4/L5 消费)",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(vitals.router)
app.include_router(mews.router)
app.include_router(alerts.router)
app.include_router(wards.router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "api-gateway"}


@app.get("/")
async def root():
    return {
        "service": "Vital Signs Monitor API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "GET /api/v1/patients/{patientId}/vitals",
            "GET /api/v1/patients/{patientId}/mews",
            "GET /api/v1/patients/{patientId}/alerts",
            "GET /api/v1/patients/{patientId}/snapshot",
            "GET /api/v1/wards/{wardId}/overview",
        ],
    }
