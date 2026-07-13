"""애플리케이션 전역 설정.

프로젝트 루트의 `.env` 파일에서 값을 읽어온다.
환경변수가 있으면 `.env`보다 우선한다.
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    APP_NAME: str = "sodosiro-ETL"
    DEBUG: bool = False


settings = Settings()
