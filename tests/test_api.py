from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def _create_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE catalog_products (
            id INTEGER PRIMARY KEY,
            canonical_product_id TEXT NOT NULL,
            parser_name TEXT NOT NULL,
            source_id TEXT NOT NULL,
            plu TEXT,
            sku TEXT,
            title_original TEXT NOT NULL,
            title_normalized_no_stopwords TEXT NOT NULL,
            brand TEXT,
            price REAL,
            discount_price REAL,
            loyal_price REAL,
            price_unit TEXT,
            rating REAL,
            reviews_count INTEGER,
            promo INTEGER,
            is_new INTEGER,
            hit INTEGER,
            adult INTEGER,
            unit TEXT NOT NULL,
            available_count REAL,
            package_quantity REAL,
            package_unit TEXT,
            composition_original TEXT,
            composition_normalized TEXT,
            primary_category_id INTEGER,
            settlement_id INTEGER,
            observed_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE catalog_product_assets (
            id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            asset_kind TEXT NOT NULL,
            sort_order INTEGER NOT NULL,
            value TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE catalog_product_snapshots (
            id INTEGER PRIMARY KEY,
            canonical_product_id TEXT NOT NULL,
            parser_name TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_run_id TEXT,
            receiver_product_id INTEGER,
            receiver_artifact_id INTEGER,
            receiver_sort_order INTEGER,
            source_event_uid TEXT,
            content_fingerprint TEXT,
            valid_from_at TEXT,
            valid_to_at TEXT,
            observed_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            price REAL,
            discount_price REAL,
            loyal_price REAL,
            price_unit TEXT,
            available_count REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE catalog_categories (
            id INTEGER PRIMARY KEY,
            category_key TEXT NOT NULL,
            parser_name TEXT NOT NULL,
            source_uid TEXT,
            parent_source_uid TEXT,
            title TEXT,
            title_normalized TEXT,
            alias TEXT,
            depth INTEGER,
            sort_order INTEGER,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE catalog_settlements (
            id INTEGER PRIMARY KEY,
            geo_key TEXT NOT NULL,
            country TEXT,
            country_normalized TEXT,
            region TEXT,
            region_normalized TEXT,
            name TEXT,
            name_normalized TEXT,
            settlement_type TEXT,
            alias TEXT,
            latitude REAL,
            longitude REAL,
            geo_point TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE converter_sync_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _seed_data(conn: sqlite3.Connection) -> None:
    now = "2026-03-01T00:00:00+00:00"
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO catalog_categories(
            id, category_key, parser_name, source_uid, parent_source_uid,
            title, title_normalized, alias, depth, sort_order,
            first_seen_at, last_seen_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "fixprice:cat-milk",
            "fixprice",
            "cat-milk",
            None,
            "Milk",
            "milk",
            "milk",
            1,
            0,
            now,
            now,
            now,
        ),
    )
    cur.execute(
        """
        INSERT INTO catalog_settlements(
            id, geo_key, country, country_normalized, region, region_normalized,
            name, name_normalized, settlement_type, alias, latitude, longitude, geo_point,
            first_seen_at, last_seen_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "RUS|MOSCOW|MOSCOW",
            "RUS",
            "rus",
            "Moscow",
            "moscow",
            "Moscow",
            "moscow",
            "city",
            "MSK",
            55.7558,
            37.6176,
            "POINT(37.6176 55.7558)",
            now,
            now,
            now,
        ),
    )

    cur.executemany(
        """
        INSERT INTO catalog_products(
            id, canonical_product_id, parser_name, source_id, plu, sku,
            title_original, title_normalized_no_stopwords, brand,
            price, discount_price, loyal_price, price_unit, rating, reviews_count,
            promo, is_new, hit, adult, unit, available_count, package_quantity, package_unit,
            composition_original, composition_normalized, primary_category_id, settlement_id,
            observed_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                1,
                "prod-1",
                "fixprice",
                "receiver:run-1:1",
                "1001",
                "sku-1001",
                "Milk 1L",
                "milk 1l",
                "Brand A",
                100.0,
                90.0,
                85.0,
                "RUB",
                4.5,
                10,
                1,
                0,
                0,
                0,
                "PCE",
                15.0,
                1.0,
                "LTR",
                "milk",
                "milk",
                1,
                1,
                "2026-02-03T09:00:00+00:00",
                now,
                now,
            ),
            (
                2,
                "prod-1",
                "chizhik",
                "receiver:run-2:1",
                "2001",
                "sku-2001",
                "Milk 1L Chizhik",
                "milk 1l chizhik",
                "Brand A",
                110.0,
                None,
                None,
                "RUB",
                4.1,
                8,
                0,
                1,
                0,
                0,
                "PCE",
                11.0,
                1.0,
                "LTR",
                "milk",
                "milk",
                1,
                1,
                "2026-02-04T09:00:00+00:00",
                now,
                now,
            ),
            (
                3,
                "prod-2",
                "fixprice",
                "receiver:run-3:1",
                "3001",
                "sku-3001",
                "Apple Juice 1L",
                "apple juice 1l",
                "Brand B",
                120.0,
                115.0,
                None,
                "RUB",
                4.0,
                3,
                0,
                0,
                1,
                0,
                "PCE",
                7.0,
                1.0,
                "LTR",
                "juice",
                "juice",
                1,
                1,
                "2026-02-05T09:00:00+00:00",
                now,
                now,
            ),
        ],
    )

    cur.executemany(
        """
        INSERT INTO catalog_product_assets(
            id, product_id, asset_kind, sort_order, value, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (1, 1, "image_url", 0, "https://img.example/p1.webp", now, now),
            (2, 2, "image_url", 0, "https://img.example/p1c.webp", now, now),
            (3, 3, "image_url", 0, "https://img.example/p2.webp", now, now),
        ],
    )

    cur.executemany(
        """
        INSERT INTO catalog_product_snapshots(
            id, canonical_product_id, parser_name, source_id,
            source_run_id, receiver_product_id, receiver_artifact_id, receiver_sort_order,
            source_event_uid, content_fingerprint, valid_from_at, valid_to_at,
            observed_at, created_at, price, discount_price, loyal_price, price_unit, available_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                101,
                "prod-1",
                "fixprice",
                "receiver:run-1:1",
                "run-1",
                1,
                1001,
                0,
                "evt-101",
                "fp-101",
                "2026-02-01T10:00:00+00:00",
                "2026-02-01T10:00:00+00:00",
                "2026-02-01T10:00:00+00:00",
                now,
                100.0,
                None,
                None,
                "RUB",
                10.0,
            ),
            (
                102,
                "prod-1",
                "fixprice",
                "receiver:run-1:1",
                "run-1",
                1,
                1001,
                1,
                "evt-102",
                "fp-102",
                "2026-02-01T15:00:00+00:00",
                "2026-02-01T15:00:00+00:00",
                "2026-02-01T15:00:00+00:00",
                now,
                90.0,
                None,
                None,
                "RUB",
                9.0,
            ),
            (
                103,
                "prod-1",
                "fixprice",
                "receiver:run-1:1",
                "run-1",
                1,
                1001,
                2,
                "evt-103",
                "fp-103",
                "2026-02-03T09:00:00+00:00",
                "2026-02-03T09:00:00+00:00",
                "2026-02-03T09:00:00+00:00",
                now,
                80.0,
                None,
                None,
                "RUB",
                8.0,
            ),
            (
                104,
                "prod-1",
                "chizhik",
                "receiver:run-2:1",
                "run-2",
                1,
                2001,
                0,
                "evt-104",
                "fp-104",
                "2026-02-02T12:00:00+00:00",
                "2026-02-02T12:00:00+00:00",
                "2026-02-02T12:00:00+00:00",
                now,
                120.0,
                None,
                None,
                "RUB",
                12.0,
            ),
            (
                201,
                "prod-interval",
                "fixprice",
                "receiver:run-interval:1",
                "run-interval",
                1,
                3001,
                0,
                "evt-201",
                "fp-201",
                "2026-02-10T10:00:00+00:00",
                "2026-02-10T12:00:00+00:00",
                "2026-02-10T12:00:00+00:00",
                now,
                77.0,
                None,
                None,
                "RUB",
                None,
            ),
        ],
    )

    cur.execute(
        """
        INSERT INTO converter_sync_state(key, value, updated_at)
        VALUES (?, ?, ?)
        """,
        ("receiver_cursor:fixprice", "2026-02-03T00:00:00+00:00\t123", now),
    )
    conn.commit()


class EndpointApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        handle = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        handle.close()
        cls.db_path = Path(handle.name)

        conn = sqlite3.connect(cls.db_path)
        try:
            _create_schema(conn)
            _seed_data(conn)
        finally:
            conn.close()

        settings = Settings(catalog_db_url=f"sqlite:///{cls.db_path}")
        cls.app = create_app(settings=settings)
        cls.client = TestClient(cls.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        cls.app.state.engine.dispose()
        cls.db_path.unlink(missing_ok=True)

    def test_healthz(self) -> None:
        response = self.client.get("/healthz")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    def test_list_products(self) -> None:
        response = self.client.get("/products", params={"limit": 10, "offset": 0})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("items", payload)
        self.assertGreaterEqual(payload["meta"]["total"], 2)
        first = payload["items"][0]
        self.assertIn("canonical_product_id", first)
        self.assertIn("parsers", first)

    def test_list_products_supports_observed_at_asc(self) -> None:
        response = self.client.get(
            "/products",
            params={"limit": 10, "offset": 0, "sort": "observed_at_asc"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertGreaterEqual(len(payload["items"]), 2)
        self.assertEqual(payload["items"][0]["canonical_product_id"], "prod-1")
        self.assertEqual(payload["items"][1]["canonical_product_id"], "prod-2")

    def test_sources_and_snapshots(self) -> None:
        sources = self.client.get("/products/prod-1/sources", params={"limit": 10, "offset": 0})
        self.assertEqual(sources.status_code, 200)
        sources_payload = sources.json()
        self.assertEqual(sources_payload["meta"]["total"], 2)
        self.assertTrue(sources_payload["items"][0]["image_urls"])

        snapshots = self.client.get(
            "/products/prod-1/snapshots",
            params={"limit": 10, "offset": 0, "parser_name": "fixprice"},
        )
        self.assertEqual(snapshots.status_code, 200)
        snapshots_payload = snapshots.json()
        self.assertEqual(snapshots_payload["meta"]["total"], 3)
        first_snapshot = snapshots_payload["items"][0]
        self.assertEqual(first_snapshot["source_event_uid"], "evt-103")
        self.assertEqual(first_snapshot["receiver_product_id"], 1)
        self.assertEqual(first_snapshot["valid_from_at"], "2026-02-03T09:00:00Z")
        self.assertEqual(first_snapshot["valid_to_at"], "2026-02-03T09:00:00Z")

    def test_categories_settlements_and_cursors(self) -> None:
        categories = self.client.get("/categories", params={"limit": 10, "offset": 0})
        self.assertEqual(categories.status_code, 200)
        self.assertGreaterEqual(categories.json()["meta"]["total"], 1)

        settlements = self.client.get("/settlements", params={"limit": 10, "offset": 0})
        self.assertEqual(settlements.status_code, 200)
        self.assertGreaterEqual(settlements.json()["meta"]["total"], 1)

        cursors = self.client.get("/sync/cursors", params={"parser_name": "fixprice"})
        self.assertEqual(cursors.status_code, 200)
        self.assertEqual(len(cursors.json()["items"]), 1)
        self.assertEqual(cursors.json()["items"][0]["product_id"], 123)

    def test_dynamics_returns_arrays_with_null_gaps(self) -> None:
        response = self.client.get(
            "/products/prod-1/dynamics",
            params={
                "parser_name": "fixprice",
                "source_id": "receiver:run-1:1",
                "field": "price",
                "interval": "1d",
                "date_from": "2026-02-01T00:00:00Z",
                "date_to": "2026-02-03T23:59:59Z",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["dates"], ["2026-02-01T00:00:00Z", "2026-02-02T00:00:00Z", "2026-02-03T00:00:00Z"])
        self.assertEqual(payload["values"], [90.0, None, 80.0])
        self.assertEqual(len(payload["dates"]), len(payload["values"]))

    def test_dynamics_bucket_uses_latest_snapshot_value(self) -> None:
        response = self.client.get(
            "/products/prod-1/dynamics",
            params={
                "parser_name": "fixprice",
                "source_id": "receiver:run-1:1",
                "field": "price",
                "interval": "1d",
                "date_from": "2026-02-01T00:00:00Z",
                "date_to": "2026-02-01T23:59:59Z",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["values"], [90.0])

    def test_dynamics_uses_valid_interval_for_reused_snapshot(self) -> None:
        response = self.client.get(
            "/products/prod-interval/dynamics",
            params={
                "parser_name": "fixprice",
                "source_id": "receiver:run-interval:1",
                "field": "price",
                "interval": "1h",
                "date_from": "2026-02-10T10:00:00Z",
                "date_to": "2026-02-10T12:00:00Z",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            payload["dates"],
            ["2026-02-10T10:00:00Z", "2026-02-10T11:00:00Z", "2026-02-10T12:00:00Z"],
        )
        self.assertEqual(payload["values"], [77.0, 77.0, 77.0])

    def test_dynamics_invalid_field_returns_400(self) -> None:
        response = self.client.get(
            "/products/prod-1/dynamics",
            params={
                "parser_name": "fixprice",
                "source_id": "receiver:run-1:1",
                "field": "unknown_metric",
                "interval": "1d",
                "date_from": "2026-02-01T00:00:00Z",
                "date_to": "2026-02-03T23:59:59Z",
            },
        )
        self.assertEqual(response.status_code, 400)

    def test_dynamics_invalid_interval_returns_400(self) -> None:
        response = self.client.get(
            "/products/prod-1/dynamics",
            params={
                "parser_name": "fixprice",
                "source_id": "receiver:run-1:1",
                "field": "price",
                "interval": "12h",
                "date_from": "2026-02-01T00:00:00Z",
                "date_to": "2026-02-03T23:59:59Z",
            },
        )
        self.assertEqual(response.status_code, 400)

    def test_dynamics_invalid_date_range_returns_422(self) -> None:
        response = self.client.get(
            "/products/prod-1/dynamics",
            params={
                "parser_name": "fixprice",
                "source_id": "receiver:run-1:1",
                "field": "price",
                "interval": "1d",
                "date_from": "2026-02-04T00:00:00Z",
                "date_to": "2026-02-03T23:59:59Z",
            },
        )
        self.assertEqual(response.status_code, 422)

    def test_dynamics_unknown_source_returns_404(self) -> None:
        response = self.client.get(
            "/products/prod-1/dynamics",
            params={
                "parser_name": "fixprice",
                "source_id": "receiver:missing",
                "field": "price",
                "interval": "1d",
                "date_from": "2026-02-01T00:00:00Z",
                "date_to": "2026-02-03T23:59:59Z",
            },
        )
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
