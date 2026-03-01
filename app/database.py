from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def create_catalog_engine(database_url: str) -> Engine:
    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    return create_engine(
        database_url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )


def ping_catalog(engine: Engine) -> None:
    with engine.connect() as connection:
        # Readiness requires access to the main projection table.
        connection.execute(text("SELECT 1 FROM catalog_products LIMIT 1"))
