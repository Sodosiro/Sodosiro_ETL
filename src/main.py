"""FastAPI 진입점.

실행: uvicorn src.main:app --reload
"""
from __future__ import annotations

from fastapi import FastAPI

from src.core.config import settings

app = FastAPI(title=settings.APP_NAME)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
