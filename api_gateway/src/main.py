"""API 网关入口。"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from src.routers import vitals, mews, alerts, wards
import os

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
        "test": "/test",
        "endpoints": [
            "GET /api/v1/patients/{patientId}/vitals",
            "GET /api/v1/patients/{patientId}/mews",
            "GET /api/v1/patients/{patientId}/alerts",
            "GET /api/v1/patients/{patientId}/snapshot",
            "GET /api/v1/wards/{wardId}/overview",
            "GET /test  (8患者测试面板)",
        ],
    }

@app.get("/test", response_class=HTMLResponse)
async def test_dashboard():
    html_path = "/app/test_dashboard.html"
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    return f"<h1>文件未找到: {html_path}</h1>"
