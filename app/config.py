from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    catalog_db_url: str
    default_page_size: int = 20
    max_page_size: int = 100


def load_settings() -> Settings:
    database_url = (os.getenv("ENDPOINT_CATALOG_DB") or os.getenv("CATALOG_DB") or "").strip()
    if not database_url:
        database_url = "sqlite:///./catalog.db"
    return Settings(catalog_db_url=database_url)
