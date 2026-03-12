from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, status

from .config import Settings, load_settings
from .database import create_catalog_engine
from .repository import CatalogReadRepository
from .schemas import parse_iso_datetime_utc


def create_app(settings: Settings | None = None) -> FastAPI:
    runtime_settings = settings or load_settings()
    engine = create_catalog_engine(runtime_settings.catalog_db_url)
    repository = CatalogReadRepository(engine)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            engine.dispose()

    app = FastAPI(
        title="Endpoint Read-Only API",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = runtime_settings
    app.state.engine = engine
    app.state.repository = repository

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        try:
            repository.health_check()
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"catalog_unavailable: {exc}",
            ) from exc
        return {"status": "ok"}

    @app.get("/products")
    def list_products(
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0),
        q: str | None = Query(None),
        parser_name: str | None = Query(None),
        category_id: int | None = Query(None),
        settlement_id: int | None = Query(None),
        brand: str | None = Query(None),
        promo: bool | None = Query(None),
        is_new: bool | None = Query(None),
        hit: bool | None = Query(None),
        adult: bool | None = Query(None),
        price_min: float | None = Query(None),
        price_max: float | None = Query(None),
        sort: str = Query("observed_at_desc"),
    ) -> dict[str, object]:
        try:
            return repository.list_products(
                limit=limit,
                offset=offset,
                q=q,
                parser_name=parser_name,
                category_id=category_id,
                settlement_id=settlement_id,
                brand=brand,
                promo=promo,
                is_new=is_new,
                hit=hit,
                adult=adult,
                price_min=price_min,
                price_max=price_max,
                sort=sort,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    @app.get("/products/count")
    def count_products(
        store: list[str] | None = Query(None),
        region: list[str] | None = Query(None),
    ) -> dict[str, object]:
        return repository.count_products(stores=store, regions=region)

    @app.get("/products/{canonical_product_id}")
    def get_product(canonical_product_id: str) -> dict[str, object]:
        payload = repository.get_product(canonical_product_id)
        if payload is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="product_not_found")
        return payload

    @app.get("/products/{canonical_product_id}/sources")
    def list_product_sources(
        canonical_product_id: str,
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0),
        sort: str = Query("observed_at_desc"),
    ) -> dict[str, object]:
        try:
            payload = repository.list_sources(
                canonical_product_id=canonical_product_id,
                limit=limit,
                offset=offset,
                sort=sort,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        if payload["meta"]["total"] == 0 and not repository.has_canonical(canonical_product_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="product_not_found")
        return payload

    @app.get("/products/{canonical_product_id}/snapshots")
    def list_product_snapshots(
        canonical_product_id: str,
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0),
        parser_name: str | None = Query(None),
        source_id: str | None = Query(None),
        sort: str = Query("observed_at_desc"),
    ) -> dict[str, object]:
        try:
            payload = repository.list_snapshots(
                canonical_product_id=canonical_product_id,
                limit=limit,
                offset=offset,
                parser_name=parser_name,
                source_id=source_id,
                sort=sort,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        if payload["meta"]["total"] == 0 and not repository.has_canonical(canonical_product_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="product_not_found")
        return payload

    @app.get("/products/{canonical_product_id}/dynamics")
    def get_product_dynamics(
        canonical_product_id: str,
        parser_name: str = Query(..., min_length=1),
        source_id: str = Query(..., min_length=1),
        field: str = Query(..., min_length=1),
        date_from: str = Query(..., min_length=1),
        date_to: str = Query(..., min_length=1),
        interval: str = Query(..., min_length=1),
    ) -> dict[str, object]:
        try:
            parsed_from = parse_iso_datetime_utc(date_from)
            parsed_to = parse_iso_datetime_utc(date_to)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc

        if parsed_from > parsed_to:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="date_from must be <= date_to",
            )

        try:
            return repository.get_dynamics_series(
                canonical_product_id=canonical_product_id,
                parser_name=parser_name,
                source_id=source_id,
                field=field,
                date_from=parsed_from,
                date_to=parsed_to,
                interval=interval,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @app.get("/products/common/count")
    def count_common_products(
        scope: str = Query("all_stores"),
        store: list[str] | None = Query(None),
        region: list[str] | None = Query(None),
    ) -> dict[str, object]:
        try:
            return repository.count_common_products(
                scope=scope,
                stores=store,
                regions=region,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    @app.get("/categories")
    def list_categories(
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0),
        parser_name: str | None = Query(None),
        depth: int | None = Query(None),
        parent_source_uid: str | None = Query(None),
        q: str | None = Query(None),
    ) -> dict[str, object]:
        return repository.list_categories(
            limit=limit,
            offset=offset,
            parser_name=parser_name,
            depth=depth,
            parent_source_uid=parent_source_uid,
            q=q,
        )

    @app.get("/categories/{category_id}")
    def get_category(category_id: int) -> dict[str, object]:
        payload = repository.get_category(category_id)
        if payload is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="category_not_found")
        return payload

    @app.get("/settlements")
    def list_settlements(
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0),
        country: str | None = Query(None),
        region: str | None = Query(None),
        q: str | None = Query(None),
    ) -> dict[str, object]:
        return repository.list_settlements(
            limit=limit,
            offset=offset,
            country=country,
            region=region,
            q=q,
        )

    @app.get("/settlements/{settlement_id}")
    def get_settlement(settlement_id: int) -> dict[str, object]:
        payload = repository.get_settlement(settlement_id)
        if payload is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="settlement_not_found")
        return payload

    @app.get("/sync/cursors")
    def list_sync_cursors(parser_name: str | None = Query(None)) -> dict[str, object]:
        return repository.list_sync_cursors(parser_name=parser_name)

    return app


app = create_app()
