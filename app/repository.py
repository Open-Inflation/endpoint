from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from .schemas import DynamicsField, DynamicsInterval, INTERVAL_SECONDS, coerce_datetime, to_iso_z


class CatalogReadRepository:
    PRODUCT_SORTS: dict[str, str] = {
        "observed_at_desc": "c.observed_at_latest DESC, c.canonical_product_id ASC",
        "observed_at_asc": "c.observed_at_latest ASC, c.canonical_product_id ASC",
        "price_asc": "(c.price_from IS NULL) ASC, c.price_from ASC, c.canonical_product_id ASC",
        "price_desc": "(c.price_from IS NULL) ASC, c.price_from DESC, c.canonical_product_id ASC",
        "rating_desc": "(r.rating IS NULL) ASC, r.rating DESC, c.canonical_product_id ASC",
    }
    SOURCE_SORTS: dict[str, str] = {
        "observed_at_desc": "cp.observed_at DESC, cp.id ASC",
        "observed_at_asc": "cp.observed_at ASC, cp.id ASC",
    }
    SNAPSHOT_SORTS: dict[str, str] = {
        "observed_at_desc": "s.observed_at DESC, s.id DESC",
        "observed_at_asc": "s.observed_at ASC, s.id ASC",
    }

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def health_check(self) -> None:
        with self._engine.connect() as connection:
            connection.execute(text("SELECT 1 FROM catalog_products LIMIT 1"))

    def has_canonical(self, canonical_product_id: str) -> bool:
        sql = text(
            """
            SELECT 1 AS hit
            FROM catalog_products
            WHERE canonical_product_id = :canonical_product_id
            LIMIT 1
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(sql, {"canonical_product_id": canonical_product_id}).first()
            if row is not None:
                return True

        sql_snapshots = text(
            """
            SELECT 1 AS hit
            FROM catalog_product_snapshots
            WHERE canonical_product_id = :canonical_product_id
            LIMIT 1
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(sql_snapshots, {"canonical_product_id": canonical_product_id}).first()
            return row is not None

    def list_products(
        self,
        *,
        limit: int,
        offset: int,
        sort: str,
        canonical_product_id: str | None = None,
        q: str | None = None,
        parser_name: str | None = None,
        category_id: int | None = None,
        settlement_id: int | None = None,
        brand: str | None = None,
        promo: bool | None = None,
        is_new: bool | None = None,
        hit: bool | None = None,
        adult: bool | None = None,
        price_min: float | None = None,
        price_max: float | None = None,
    ) -> dict[str, Any]:
        sort_sql = self.PRODUCT_SORTS.get(sort)
        if sort_sql is None:
            raise ValueError(f"unsupported sort: {sort}")
        parsers_agg_sql = self._sql_parsers_aggregate_expression()

        where_sql, params = self._build_products_where(
            canonical_product_id=canonical_product_id,
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
        )
        params["limit"] = int(limit)
        params["offset"] = int(offset)

        count_sql = text(
            f"""
            WITH filtered AS (
                SELECT cp.*
                FROM catalog_products cp
                {where_sql}
            )
            SELECT COUNT(*) AS total
            FROM (
                SELECT canonical_product_id
                FROM filtered
                GROUP BY canonical_product_id
            ) grouped
            """
        )

        query_sql = text(
            f"""
            WITH filtered AS (
                SELECT cp.*
                FROM catalog_products cp
                {where_sql}
            ),
            canonicals AS (
                SELECT
                    canonical_product_id,
                    COUNT(*) AS sources_count,
                    MAX(observed_at) AS observed_at_latest,
                    MIN(COALESCE(discount_price, loyal_price, price)) AS price_from,
                    {parsers_agg_sql} AS parsers_csv
                FROM filtered
                GROUP BY canonical_product_id
            ),
            ranked AS (
                SELECT
                    f.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.canonical_product_id
                        ORDER BY f.observed_at DESC, f.updated_at DESC, f.id ASC
                    ) AS rn
                FROM filtered f
            )
            SELECT
                c.canonical_product_id,
                r.title_original AS title,
                r.brand,
                c.price_from,
                r.price_unit,
                c.sources_count,
                c.observed_at_latest,
                c.parsers_csv,
                r.id AS representative_product_id,
                r.rating
            FROM canonicals c
            JOIN ranked r
              ON r.canonical_product_id = c.canonical_product_id
             AND r.rn = 1
            ORDER BY {sort_sql}
            LIMIT :limit OFFSET :offset
            """
        )

        with self._engine.connect() as connection:
            total = int(connection.execute(count_sql, params).scalar_one())
            rows = connection.execute(query_sql, params).mappings().all()

        representative_ids = [
            int(row["representative_product_id"])
            for row in rows
            if row.get("representative_product_id") is not None
        ]
        image_by_product = self._load_primary_image_urls(representative_ids)

        items: list[dict[str, Any]] = []
        for row in rows:
            observed_at = coerce_datetime(row["observed_at_latest"])
            representative_id = int(row["representative_product_id"])
            parsers = self._split_csv(row.get("parsers_csv"))
            items.append(
                {
                    "canonical_product_id": str(row["canonical_product_id"]),
                    "title": self._safe_str(row.get("title")),
                    "brand": self._safe_str(row.get("brand")),
                    "price_from": self._as_float(row.get("price_from")),
                    "price_unit": self._safe_str(row.get("price_unit")),
                    "sources_count": int(row.get("sources_count") or 0),
                    "observed_at_latest": to_iso_z(observed_at),
                    "image_url": image_by_product.get(representative_id),
                    "parsers": parsers,
                }
            )

        return {
            "items": items,
            "meta": {
                "limit": int(limit),
                "offset": int(offset),
                "total": total,
            },
        }

    def get_product(self, canonical_product_id: str) -> dict[str, Any] | None:
        payload = self.list_products(
            limit=1,
            offset=0,
            sort="observed_at_desc",
            canonical_product_id=canonical_product_id,
        )
        items = payload["items"]
        if not items:
            return None
        return items[0]

    def list_sources(
        self,
        *,
        canonical_product_id: str,
        limit: int,
        offset: int,
        sort: str,
    ) -> dict[str, Any]:
        sort_sql = self.SOURCE_SORTS.get(sort)
        if sort_sql is None:
            raise ValueError(f"unsupported sort: {sort}")

        count_sql = text(
            """
            SELECT COUNT(*) AS total
            FROM catalog_products
            WHERE canonical_product_id = :canonical_product_id
            """
        )

        query_sql = text(
            f"""
            SELECT
                cp.*,
                cc.id AS category_id,
                cc.source_uid AS category_source_uid,
                cc.title AS category_title,
                cs.id AS settlement_id_joined,
                cs.name AS settlement_name,
                cs.region AS settlement_region,
                cs.country AS settlement_country
            FROM catalog_products cp
            LEFT JOIN catalog_categories cc
              ON cc.id = cp.primary_category_id
            LEFT JOIN catalog_settlements cs
              ON cs.id = cp.settlement_id
            WHERE cp.canonical_product_id = :canonical_product_id
            ORDER BY {sort_sql}
            LIMIT :limit OFFSET :offset
            """
        )

        params = {
            "canonical_product_id": canonical_product_id,
            "limit": int(limit),
            "offset": int(offset),
        }
        with self._engine.connect() as connection:
            total = int(connection.execute(count_sql, params).scalar_one())
            rows = connection.execute(query_sql, params).mappings().all()

        product_ids = [int(row["id"]) for row in rows]
        images_by_product = self._load_image_assets(product_ids)

        items: list[dict[str, Any]] = []
        for row in rows:
            observed_at = coerce_datetime(row["observed_at"])
            item = {
                "id": int(row["id"]),
                "canonical_product_id": str(row["canonical_product_id"]),
                "parser_name": self._safe_str(row.get("parser_name")),
                "source_id": self._safe_str(row.get("source_id")),
                "plu": self._safe_str(row.get("plu")),
                "sku": self._safe_str(row.get("sku")),
                "title_original": self._safe_str(row.get("title_original")),
                "title_normalized_no_stopwords": self._safe_str(row.get("title_normalized_no_stopwords")),
                "brand": self._safe_str(row.get("brand")),
                "price": self._as_float(row.get("price")),
                "discount_price": self._as_float(row.get("discount_price")),
                "loyal_price": self._as_float(row.get("loyal_price")),
                "price_unit": self._safe_str(row.get("price_unit")),
                "rating": self._as_float(row.get("rating")),
                "reviews_count": self._as_int(row.get("reviews_count")),
                "promo": self._as_bool(row.get("promo")),
                "is_new": self._as_bool(row.get("is_new")),
                "hit": self._as_bool(row.get("hit")),
                "adult": self._as_bool(row.get("adult")),
                "unit": self._safe_str(row.get("unit")),
                "available_count": self._as_float(row.get("available_count")),
                "package_quantity": self._as_float(row.get("package_quantity")),
                "package_unit": self._safe_str(row.get("package_unit")),
                "composition_original": self._safe_str(row.get("composition_original")),
                "composition_normalized": self._safe_str(row.get("composition_normalized")),
                "observed_at": to_iso_z(observed_at),
                "image_urls": images_by_product.get(int(row["id"]), []),
            }
            category_id = row.get("category_id")
            if category_id is not None:
                item["primary_category"] = {
                    "id": int(category_id),
                    "source_uid": self._safe_str(row.get("category_source_uid")),
                    "title": self._safe_str(row.get("category_title")),
                }
            else:
                item["primary_category"] = None

            settlement_id = row.get("settlement_id_joined")
            if settlement_id is not None:
                item["settlement"] = {
                    "id": int(settlement_id),
                    "name": self._safe_str(row.get("settlement_name")),
                    "region": self._safe_str(row.get("settlement_region")),
                    "country": self._safe_str(row.get("settlement_country")),
                }
            else:
                item["settlement"] = None

            items.append(item)

        return {
            "items": items,
            "meta": {
                "limit": int(limit),
                "offset": int(offset),
                "total": int(total),
            },
        }

    def list_snapshots(
        self,
        *,
        canonical_product_id: str,
        limit: int,
        offset: int,
        parser_name: str | None,
        source_id: str | None,
        sort: str,
    ) -> dict[str, Any]:
        sort_sql = self.SNAPSHOT_SORTS.get(sort)
        if sort_sql is None:
            raise ValueError(f"unsupported sort: {sort}")

        where_clauses = ["s.canonical_product_id = :canonical_product_id"]
        params: dict[str, Any] = {
            "canonical_product_id": canonical_product_id,
            "limit": int(limit),
            "offset": int(offset),
        }
        if parser_name:
            where_clauses.append("s.parser_name = :parser_name")
            params["parser_name"] = parser_name
        if source_id:
            where_clauses.append("s.source_id = :source_id")
            params["source_id"] = source_id
        where_sql = "WHERE " + " AND ".join(where_clauses)

        count_sql = text(
            f"""
            SELECT COUNT(*) AS total
            FROM catalog_product_snapshots s
            {where_sql}
            """
        )

        query_sql = text(
            f"""
            SELECT
                s.*
            FROM catalog_product_snapshots s
            {where_sql}
            ORDER BY {sort_sql}
            LIMIT :limit OFFSET :offset
            """
        )

        with self._engine.connect() as connection:
            total = int(connection.execute(count_sql, params).scalar_one())
            rows = connection.execute(query_sql, params).mappings().all()

        items: list[dict[str, Any]] = []
        for row in rows:
            observed_at = coerce_datetime(row["observed_at"])
            created_at = coerce_datetime(row["created_at"])
            valid_from_value = row.get("valid_from_at")
            valid_to_value = row.get("valid_to_at")
            items.append(
                {
                    "id": int(row["id"]),
                    "canonical_product_id": self._safe_str(row.get("canonical_product_id")),
                    "parser_name": self._safe_str(row.get("parser_name")),
                    "source_id": self._safe_str(row.get("source_id")),
                    "source_run_id": self._safe_str(row.get("source_run_id")),
                    "receiver_product_id": self._as_int(row.get("receiver_product_id")),
                    "receiver_artifact_id": self._as_int(row.get("receiver_artifact_id")),
                    "receiver_sort_order": self._as_int(row.get("receiver_sort_order")),
                    "source_event_uid": self._safe_str(row.get("source_event_uid")),
                    "content_fingerprint": self._safe_str(row.get("content_fingerprint")),
                    "price": self._as_float(row.get("price")),
                    "discount_price": self._as_float(row.get("discount_price")),
                    "loyal_price": self._as_float(row.get("loyal_price")),
                    "price_unit": self._safe_str(row.get("price_unit")),
                    "available_count": self._as_float(row.get("available_count")),
                    "valid_from_at": (
                        to_iso_z(coerce_datetime(valid_from_value))
                        if valid_from_value is not None
                        else None
                    ),
                    "valid_to_at": (
                        to_iso_z(coerce_datetime(valid_to_value))
                        if valid_to_value is not None
                        else None
                    ),
                    "observed_at": to_iso_z(observed_at),
                    "created_at": to_iso_z(created_at),
                }
            )

        return {
            "items": items,
            "meta": {
                "limit": int(limit),
                "offset": int(offset),
                "total": int(total),
            },
        }

    def list_categories(
        self,
        *,
        limit: int,
        offset: int,
        parser_name: str | None,
        depth: int | None,
        parent_source_uid: str | None,
        q: str | None,
    ) -> dict[str, Any]:
        where_clauses: list[str] = []
        params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}

        if parser_name:
            where_clauses.append("parser_name = :parser_name")
            params["parser_name"] = parser_name
        if depth is not None:
            where_clauses.append("depth = :depth")
            params["depth"] = int(depth)
        if parent_source_uid:
            where_clauses.append("parent_source_uid = :parent_source_uid")
            params["parent_source_uid"] = parent_source_uid
        if q:
            where_clauses.append(
                "("
                "LOWER(COALESCE(title, '')) LIKE :q "
                "OR LOWER(COALESCE(title_normalized, '')) LIKE :q "
                "OR LOWER(COALESCE(alias, '')) LIKE :q "
                "OR LOWER(COALESCE(source_uid, '')) LIKE :q"
                ")"
            )
            params["q"] = f"%{q.strip().lower()}%"

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        count_sql = text(f"SELECT COUNT(*) AS total FROM catalog_categories {where_sql}")
        query_sql = text(
            f"""
            SELECT *
            FROM catalog_categories
            {where_sql}
            ORDER BY updated_at DESC, id ASC
            LIMIT :limit OFFSET :offset
            """
        )

        with self._engine.connect() as connection:
            total = int(connection.execute(count_sql, params).scalar_one())
            rows = connection.execute(query_sql, params).mappings().all()

        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(self._serialize_category_row(row))

        return {
            "items": items,
            "meta": {
                "limit": int(limit),
                "offset": int(offset),
                "total": int(total),
            },
        }

    def get_category(self, category_id: int) -> dict[str, Any] | None:
        sql = text("SELECT * FROM catalog_categories WHERE id = :category_id LIMIT 1")
        with self._engine.connect() as connection:
            row = connection.execute(sql, {"category_id": int(category_id)}).mappings().first()
        if row is None:
            return None
        return self._serialize_category_row(row)

    def list_settlements(
        self,
        *,
        limit: int,
        offset: int,
        country: str | None,
        region: str | None,
        q: str | None,
    ) -> dict[str, Any]:
        where_clauses: list[str] = []
        params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}

        if country:
            where_clauses.append("country = :country")
            params["country"] = country
        if region:
            where_clauses.append("region = :region")
            params["region"] = region
        if q:
            where_clauses.append(
                "("
                "LOWER(COALESCE(name, '')) LIKE :q "
                "OR LOWER(COALESCE(alias, '')) LIKE :q "
                "OR LOWER(COALESCE(region, '')) LIKE :q "
                "OR LOWER(COALESCE(country, '')) LIKE :q"
                ")"
            )
            params["q"] = f"%{q.strip().lower()}%"

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        count_sql = text(f"SELECT COUNT(*) AS total FROM catalog_settlements {where_sql}")
        query_sql = text(
            f"""
            SELECT *
            FROM catalog_settlements
            {where_sql}
            ORDER BY updated_at DESC, id ASC
            LIMIT :limit OFFSET :offset
            """
        )

        with self._engine.connect() as connection:
            total = int(connection.execute(count_sql, params).scalar_one())
            rows = connection.execute(query_sql, params).mappings().all()

        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(self._serialize_settlement_row(row))

        return {
            "items": items,
            "meta": {
                "limit": int(limit),
                "offset": int(offset),
                "total": int(total),
            },
        }

    def get_settlement(self, settlement_id: int) -> dict[str, Any] | None:
        sql = text("SELECT * FROM catalog_settlements WHERE id = :settlement_id LIMIT 1")
        with self._engine.connect() as connection:
            row = connection.execute(sql, {"settlement_id": int(settlement_id)}).mappings().first()
        if row is None:
            return None
        return self._serialize_settlement_row(row)

    def list_sync_cursors(self, *, parser_name: str | None = None) -> dict[str, Any]:
        inspector = inspect(self._engine)
        if not inspector.has_table("converter_sync_state"):
            return {"items": []}

        params: dict[str, Any] = {}
        where_sql = 'WHERE "key" LIKE :prefix'
        params["prefix"] = "receiver_cursor:%"

        if parser_name:
            where_sql = 'WHERE "key" = :exact_key'
            params["exact_key"] = f"receiver_cursor:{parser_name.strip().lower()}"

        sql = text(
            f"""
            SELECT "key" AS state_key, value, updated_at
            FROM converter_sync_state
            {where_sql}
            ORDER BY state_key ASC
            """
        )

        with self._engine.connect() as connection:
            rows = connection.execute(sql, params).mappings().all()

        items: list[dict[str, Any]] = []
        for row in rows:
            state_key = self._safe_str(row.get("state_key")) or ""
            parser = state_key.removeprefix("receiver_cursor:")
            raw_value = self._safe_str(row.get("value")) or ""
            ingested_at, product_id = self._parse_cursor_value(raw_value)
            updated_at_value = row.get("updated_at")
            updated_at = None
            if updated_at_value is not None:
                updated_at = to_iso_z(coerce_datetime(updated_at_value))

            items.append(
                {
                    "parser_name": parser,
                    "ingested_at": ingested_at,
                    "product_id": product_id,
                    "updated_at": updated_at,
                }
            )

        return {"items": items}

    def count_common_products(
        self,
        *,
        scope: str,
        stores: list[str] | None = None,
        regions: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_scope = (scope or "").strip().lower()
        if normalized_scope not in {"stores", "regions"}:
            raise ValueError("scope must be one of: stores, regions")

        normalized_stores = self._normalize_filter_tokens(stores)
        normalized_regions = self._normalize_filter_tokens(regions)

        if normalized_scope == "stores":
            common_count, required_stores = self._count_common_by_stores(
                stores=normalized_stores,
                regions=normalized_regions,
            )
            return {
                "scope": normalized_scope,
                "common_products_count": common_count,
                "required_stores": required_stores,
                "required_regions": None,
                "stores": normalized_stores or None,
                "regions": normalized_regions or None,
            }

        common_count, required_regions = self._count_common_by_regions(
            regions=normalized_regions,
            stores=normalized_stores,
        )
        return {
            "scope": normalized_scope,
            "common_products_count": common_count,
            "required_stores": None,
            "required_regions": required_regions,
            "stores": normalized_stores or None,
            "regions": normalized_regions or None,
        }

    def _count_common_by_stores(
        self,
        *,
        stores: list[str],
        regions: list[str],
    ) -> tuple[int, int]:
        params: dict[str, Any] = {}
        where_clauses = ["cp.canonical_product_id IS NOT NULL"]
        if stores:
            store_in_clause = self._build_text_in_clause("store", stores, params)
            where_clauses.append(f"LOWER(cp.parser_name) IN ({store_in_clause})")

        join_sql = ""
        if regions:
            join_sql = "LEFT JOIN catalog_settlements cs ON cs.id = cp.settlement_id"
            region_expr = "LOWER(COALESCE(cs.region_normalized, cs.region, ''))"
            region_in_clause = self._build_text_in_clause("reg", regions, params)
            where_clauses.append(f"{region_expr} IN ({region_in_clause})")

        where_sql = "WHERE " + " AND ".join(where_clauses)
        required_sql = (
            "SELECT :required_stores AS required_stores"
            if stores
            else "SELECT COUNT(DISTINCT parser_name) AS required_stores FROM base"
        )
        if stores:
            params["required_stores"] = len(stores)

        sql = text(
            f"""
            WITH base AS (
                SELECT DISTINCT cp.canonical_product_id, LOWER(cp.parser_name) AS parser_name
                FROM catalog_products cp
                {join_sql}
                {where_sql}
            ),
            required AS (
                {required_sql}
            ),
            overlap AS (
                SELECT canonical_product_id
                FROM base
                GROUP BY canonical_product_id
                HAVING COUNT(DISTINCT parser_name) = (SELECT required_stores FROM required)
                   AND (SELECT required_stores FROM required) > 0
            )
            SELECT
                COALESCE((SELECT required_stores FROM required), 0) AS required_stores,
                COUNT(*) AS common_products_count
            FROM overlap
            """
        )

        with self._engine.connect() as connection:
            row = connection.execute(sql, params).mappings().one()
        return (
            int(row.get("common_products_count") or 0),
            int(row.get("required_stores") or 0),
        )

    def _count_common_by_regions(
        self,
        *,
        regions: list[str],
        stores: list[str],
    ) -> tuple[int, int]:
        params: dict[str, Any] = {}
        region_expr = "LOWER(COALESCE(cs.region_normalized, cs.region, ''))"
        where_clauses = [
            "cp.canonical_product_id IS NOT NULL",
            f"{region_expr} <> ''",
        ]
        if regions:
            region_in_clause = self._build_text_in_clause("reg", regions, params)
            where_clauses.append(f"{region_expr} IN ({region_in_clause})")
        if stores:
            store_in_clause = self._build_text_in_clause("store", stores, params)
            where_clauses.append(f"LOWER(cp.parser_name) IN ({store_in_clause})")

        where_sql = "WHERE " + " AND ".join(where_clauses)
        required_sql = (
            "SELECT :required_regions AS required_regions"
            if regions
            else "SELECT COUNT(DISTINCT region_key) AS required_regions FROM base"
        )
        if regions:
            params["required_regions"] = len(regions)

        sql = text(
            f"""
            WITH base AS (
                SELECT DISTINCT cp.canonical_product_id, {region_expr} AS region_key
                FROM catalog_products cp
                JOIN catalog_settlements cs
                  ON cs.id = cp.settlement_id
                {where_sql}
            ),
            required AS (
                {required_sql}
            ),
            overlap AS (
                SELECT canonical_product_id
                FROM base
                GROUP BY canonical_product_id
                HAVING COUNT(DISTINCT region_key) = (SELECT required_regions FROM required)
                   AND (SELECT required_regions FROM required) > 0
            )
            SELECT
                COALESCE((SELECT required_regions FROM required), 0) AS required_regions,
                COUNT(*) AS common_products_count
            FROM overlap
            """
        )

        with self._engine.connect() as connection:
            row = connection.execute(sql, params).mappings().one()
        return (
            int(row.get("common_products_count") or 0),
            int(row.get("required_regions") or 0),
        )

    def get_dynamics_series(
        self,
        *,
        canonical_product_id: str,
        parser_name: str,
        source_id: str,
        field: str,
        date_from: datetime,
        date_to: datetime,
        interval: str,
    ) -> dict[str, Any]:
        if field not in {item.value for item in DynamicsField}:
            raise ValueError(f"unsupported field: {field}")
        if interval not in {item.value for item in DynamicsInterval}:
            raise ValueError(f"unsupported interval: {interval}")

        step_seconds = INTERVAL_SECONDS[interval]
        point_count = int(((date_to - date_from).total_seconds()) // step_seconds) + 1
        if point_count <= 0:
            raise ValueError("invalid date range")
        if point_count > 20000:
            raise ValueError("requested range is too large")

        exists_sql = text(
            """
            SELECT 1 AS hit
            FROM catalog_product_snapshots
            WHERE canonical_product_id = :canonical_product_id
              AND parser_name = :parser_name
              AND source_id = :source_id
            LIMIT 1
            """
        )
        exists_params = {
            "canonical_product_id": canonical_product_id,
            "parser_name": parser_name,
            "source_id": source_id,
        }
        with self._engine.connect() as connection:
            exists = connection.execute(exists_sql, exists_params).first()
        if exists is None:
            raise LookupError("source not found")

        # `field` is injected only from strict whitelist above.
        series_sql = text(
            f"""
            SELECT
                observed_at,
                COALESCE(valid_from_at, observed_at) AS valid_from_at,
                COALESCE(valid_to_at, observed_at) AS valid_to_at,
                {field} AS metric_value
            FROM catalog_product_snapshots
            WHERE canonical_product_id = :canonical_product_id
              AND parser_name = :parser_name
              AND source_id = :source_id
              AND COALESCE(valid_from_at, observed_at) <= :date_to
              AND COALESCE(valid_to_at, observed_at) >= :date_from
            ORDER BY observed_at ASC, id ASC
            """
        )
        params = {
            "canonical_product_id": canonical_product_id,
            "parser_name": parser_name,
            "source_id": source_id,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
        }
        with self._engine.connect() as connection:
            rows = connection.execute(series_sql, params).mappings().all()

        bucket_latest: dict[int, tuple[datetime, float | None]] = {}
        for row in rows:
            observed_at = coerce_datetime(row["observed_at"])
            valid_from_at = coerce_datetime(row["valid_from_at"])
            valid_to_at = coerce_datetime(row["valid_to_at"])
            if valid_to_at < valid_from_at:
                valid_from_at, valid_to_at = valid_to_at, valid_from_at

            interval_from = valid_from_at if valid_from_at >= date_from else date_from
            interval_to = valid_to_at if valid_to_at <= date_to else date_to
            if interval_to < interval_from:
                continue

            start_bucket = int(((interval_from - date_from).total_seconds()) // step_seconds)
            end_bucket = int(((interval_to - date_from).total_seconds()) // step_seconds)
            if end_bucket < 0 or start_bucket >= point_count:
                continue
            start_bucket = max(0, start_bucket)
            end_bucket = min(point_count - 1, end_bucket)

            value = self._as_float(row.get("metric_value"))
            for bucket in range(start_bucket, end_bucket + 1):
                current = bucket_latest.get(bucket)
                if current is None or observed_at >= current[0]:
                    bucket_latest[bucket] = (observed_at, value)

        dates: list[str] = []
        values: list[float | None] = []
        step = timedelta(seconds=step_seconds)
        for idx in range(point_count):
            point = date_from + step * idx
            dates.append(to_iso_z(point))
            bucket_value = bucket_latest.get(idx)
            values.append(bucket_value[1] if bucket_value is not None else None)

        return {
            "dates": dates,
            "values": values,
            "meta": {
                "canonical_product_id": canonical_product_id,
                "parser_name": parser_name,
                "source_id": source_id,
                "field": field,
                "interval": interval,
                "date_from": to_iso_z(date_from),
                "date_to": to_iso_z(date_to),
            },
        }

    def _load_primary_image_urls(self, product_ids: list[int]) -> dict[int, str]:
        if not product_ids:
            return {}
        params: dict[str, Any] = {}
        in_clause = self._build_in_clause("pid", product_ids, params)
        sql = text(
            f"""
            SELECT product_id, value
            FROM catalog_product_assets
            WHERE asset_kind = 'image_url'
              AND sort_order = 0
              AND product_id IN ({in_clause})
            """
        )
        with self._engine.connect() as connection:
            rows = connection.execute(sql, params).mappings().all()
        out: dict[int, str] = {}
        for row in rows:
            pid = int(row["product_id"])
            token = self._safe_str(row.get("value"))
            if token is None:
                continue
            out[pid] = token
        return out

    def _load_image_assets(self, product_ids: list[int]) -> dict[int, list[str]]:
        if not product_ids:
            return {}
        params: dict[str, Any] = {}
        in_clause = self._build_in_clause("pid", product_ids, params)
        sql = text(
            f"""
            SELECT product_id, value
            FROM catalog_product_assets
            WHERE asset_kind = 'image_url'
              AND product_id IN ({in_clause})
            ORDER BY product_id ASC, sort_order ASC
            """
        )
        with self._engine.connect() as connection:
            rows = connection.execute(sql, params).mappings().all()

        out: dict[int, list[str]] = {}
        for row in rows:
            product_id = int(row["product_id"])
            token = self._safe_str(row.get("value"))
            if token is None:
                continue
            out.setdefault(product_id, []).append(token)
        return out

    def _build_products_where(
        self,
        *,
        canonical_product_id: str | None,
        q: str | None,
        parser_name: str | None,
        category_id: int | None,
        settlement_id: int | None,
        brand: str | None,
        promo: bool | None,
        is_new: bool | None,
        hit: bool | None,
        adult: bool | None,
        price_min: float | None,
        price_max: float | None,
    ) -> tuple[str, dict[str, Any]]:
        clauses: list[str] = []
        params: dict[str, Any] = {}

        if canonical_product_id:
            clauses.append("cp.canonical_product_id = :canonical_product_id")
            params["canonical_product_id"] = canonical_product_id
        if q:
            clauses.append(
                "("
                "LOWER(COALESCE(cp.title_original, '')) LIKE :q "
                "OR LOWER(COALESCE(cp.title_normalized_no_stopwords, '')) LIKE :q "
                "OR LOWER(COALESCE(cp.brand, '')) LIKE :q"
                ")"
            )
            params["q"] = f"%{q.strip().lower()}%"
        if parser_name:
            clauses.append("cp.parser_name = :parser_name")
            params["parser_name"] = parser_name
        if category_id is not None:
            clauses.append("cp.primary_category_id = :category_id")
            params["category_id"] = int(category_id)
        if settlement_id is not None:
            clauses.append("cp.settlement_id = :settlement_id")
            params["settlement_id"] = int(settlement_id)
        if brand:
            clauses.append("LOWER(COALESCE(cp.brand, '')) = :brand")
            params["brand"] = brand.strip().lower()
        if promo is not None:
            clauses.append("cp.promo = :promo")
            params["promo"] = bool(promo)
        if is_new is not None:
            clauses.append("cp.is_new = :is_new")
            params["is_new"] = bool(is_new)
        if hit is not None:
            clauses.append("cp.hit = :hit")
            params["hit"] = bool(hit)
        if adult is not None:
            clauses.append("cp.adult = :adult")
            params["adult"] = bool(adult)
        if price_min is not None:
            clauses.append("COALESCE(cp.discount_price, cp.loyal_price, cp.price) >= :price_min")
            params["price_min"] = float(price_min)
        if price_max is not None:
            clauses.append("COALESCE(cp.discount_price, cp.loyal_price, cp.price) <= :price_max")
            params["price_max"] = float(price_max)

        if not clauses:
            return "", params
        return f"WHERE {' AND '.join(clauses)}", params

    def _sql_parsers_aggregate_expression(self) -> str:
        # Keep aggregate syntax compatible with both PostgreSQL and SQLite.
        if self._engine.dialect.name == "postgresql":
            return "STRING_AGG(DISTINCT parser_name, ',')"
        return "GROUP_CONCAT(DISTINCT parser_name)"

    @staticmethod
    def _parse_cursor_value(value: str) -> tuple[str | None, int | None]:
        if "\t" not in value:
            return None, None
        ingested_at, product_id_raw = value.rsplit("\t", 1)
        ingested = ingested_at.strip() or None
        try:
            product_id = int(product_id_raw)
        except ValueError:
            product_id = None
        return ingested, product_id

    @staticmethod
    def _build_in_clause(prefix: str, values: list[int], params: dict[str, Any]) -> str:
        placeholders: list[str] = []
        for index, value in enumerate(values):
            key = f"{prefix}_{index}"
            params[key] = int(value)
            placeholders.append(f":{key}")
        if not placeholders:
            return "NULL"
        return ", ".join(placeholders)

    @staticmethod
    def _build_text_in_clause(prefix: str, values: list[str], params: dict[str, Any]) -> str:
        placeholders: list[str] = []
        for index, value in enumerate(values):
            key = f"{prefix}_{index}"
            params[key] = str(value)
            placeholders.append(f":{key}")
        if not placeholders:
            return "NULL"
        return ", ".join(placeholders)

    @staticmethod
    def _normalize_filter_tokens(values: list[str] | None) -> list[str]:
        if not values:
            return []
        out: list[str] = []
        seen: set[str] = set()
        for raw in values:
            token = CatalogReadRepository._safe_str(raw)
            if token is None:
                continue
            normalized = token.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
        return out

    @staticmethod
    def _safe_str(value: object) -> str | None:
        if value is None:
            return None
        token = str(value).strip()
        return token or None

    @staticmethod
    def _split_csv(value: object) -> list[str]:
        token = CatalogReadRepository._safe_str(value)
        if token is None:
            return []
        items = [item.strip() for item in token.split(",") if item.strip()]
        seen: set[str] = set()
        out: list[str] = []
        for item in items:
            lowered = item.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            out.append(item)
        return out

    @staticmethod
    def _as_int(value: object) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else None
        token = str(value).strip()
        if not token:
            return None
        try:
            return int(token)
        except ValueError:
            return None

    @staticmethod
    def _as_float(value: object) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, Decimal):
            return float(value)
        token = str(value).strip()
        if not token:
            return None
        token = token.replace(",", ".")
        try:
            return float(token)
        except ValueError:
            return None

    @staticmethod
    def _as_bool(value: object) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        token = str(value).strip().lower()
        if token in {"1", "true", "yes", "y", "on"}:
            return True
        if token in {"0", "false", "no", "n", "off"}:
            return False
        return None

    @staticmethod
    def _serialize_category_row(row: Any) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "category_key": CatalogReadRepository._safe_str(row.get("category_key")),
            "parser_name": CatalogReadRepository._safe_str(row.get("parser_name")),
            "source_uid": CatalogReadRepository._safe_str(row.get("source_uid")),
            "parent_source_uid": CatalogReadRepository._safe_str(row.get("parent_source_uid")),
            "title": CatalogReadRepository._safe_str(row.get("title")),
            "title_normalized": CatalogReadRepository._safe_str(row.get("title_normalized")),
            "alias": CatalogReadRepository._safe_str(row.get("alias")),
            "depth": CatalogReadRepository._as_int(row.get("depth")),
            "sort_order": CatalogReadRepository._as_int(row.get("sort_order")),
            "first_seen_at": to_iso_z(coerce_datetime(row["first_seen_at"])),
            "last_seen_at": to_iso_z(coerce_datetime(row["last_seen_at"])),
            "updated_at": to_iso_z(coerce_datetime(row["updated_at"])),
        }

    @staticmethod
    def _serialize_settlement_row(row: Any) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "geo_key": CatalogReadRepository._safe_str(row.get("geo_key")),
            "country": CatalogReadRepository._safe_str(row.get("country")),
            "country_normalized": CatalogReadRepository._safe_str(row.get("country_normalized")),
            "region": CatalogReadRepository._safe_str(row.get("region")),
            "region_normalized": CatalogReadRepository._safe_str(row.get("region_normalized")),
            "name": CatalogReadRepository._safe_str(row.get("name")),
            "name_normalized": CatalogReadRepository._safe_str(row.get("name_normalized")),
            "settlement_type": CatalogReadRepository._safe_str(row.get("settlement_type")),
            "alias": CatalogReadRepository._safe_str(row.get("alias")),
            "latitude": CatalogReadRepository._as_float(row.get("latitude")),
            "longitude": CatalogReadRepository._as_float(row.get("longitude")),
            "first_seen_at": to_iso_z(coerce_datetime(row["first_seen_at"])),
            "last_seen_at": to_iso_z(coerce_datetime(row["last_seen_at"])),
            "updated_at": to_iso_z(coerce_datetime(row["updated_at"])),
        }
