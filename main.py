# main.py
from fastapi import FastAPI
from app_dashboard import router as dashboard_router

app = FastAPI(title="Dashboard test")

app.include_router(dashboard_router)
