"""
base.py — Shared Supabase client, retry logic, crawl logging, and config.

All crawlers import from here. The SupabaseWriter wraps supabase-py to provide
upsert-with-conflict and truncate-and-insert patterns. CrawlLogger and CrawlConfig
read/write the crawl_config and crawl_log tables.
"""

import time
import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)


def retry_with_backoff(fn, max_attempts: int = 3, base_delay: float = 2.0):
    """
    Call fn() up to max_attempts times with exponential backoff.
    Raises the last exception if all attempts fail.
    """
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                log.warning("Attempt %d/%d failed: %s. Retrying in %.1fs",
                            attempt + 1, max_attempts, exc, delay)
                time.sleep(delay)
    raise last_exc


class SupabaseWriter:
    """
    Wraps a supabase.Client to provide upsert and truncate-and-insert.
    Accepts any supabase-py compatible client (or a mock in tests).
    """

    def __init__(self, client):
        self._client = client

    def upsert(self, table: str, records: list[dict], on_conflict: str) -> int:
        """
        Upsert records into table using the given conflict column list.
        Returns the number of rows affected.
        on_conflict: comma-separated column names, e.g. "station_id,recorded_at"
        """
        if not records:
            return 0
        result = (
            self._client.table(table)
            .upsert(records, on_conflict=on_conflict)
            .execute()
        )
        return len(result.data) if result.data else 0

    def truncate_and_insert(self, table: str, records: list[dict]) -> int:
        """
        Atomically truncate table then insert all records.
        Uses a Supabase RPC function 'truncate_table' that must be created in
        the database (see 004_rpc_functions.sql).

        Required SQL (run once in Supabase SQL editor):
            CREATE OR REPLACE FUNCTION truncate_table(table_name TEXT)
            RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
            BEGIN EXECUTE 'TRUNCATE TABLE ' || quote_ident(table_name); END; $$;
        """
        self._client.rpc("truncate_table", {"table_name": table}).execute()
        if not records:
            return 0
        result = self._client.table(table).insert(records).execute()
        return len(result.data) if result.data else 0


class CrawlLogger:
    """Writes start/finish rows to crawl_log for audit trail."""

    def __init__(self, client, source_name: str):
        self._client = client
        self._source = source_name

    def start(self) -> int:
        """Insert a 'running' row. Returns the new log row id."""
        result = (
            self._client.table("crawl_log")
            .insert({
                "source_name": self._source,
                "started_at": _now_iso(),
                "status": "running",
            })
            .execute()
        )
        return result.data[0]["id"]

    def finish(self, log_id: int, records_upserted: int, status: str,
               error_message: str | None = None):
        """Update the log row with outcome."""
        self._client.table("crawl_log").update({
            "completed_at": _now_iso(),
            "records_upserted": records_upserted,
            "status": status,
            "error_message": error_message,
        }).eq("id", log_id).execute()


class CrawlConfig:
    """Reads and writes crawl_config rows for a given source."""

    def __init__(self, client, source_name: str):
        self._client = client
        self._source = source_name

    def get(self) -> dict:
        """Fetch the config row for this source."""
        result = (
            self._client.table("crawl_config")
            .select("*")
            .eq("source_name", self._source)
            .single()
            .execute()
        )
        return result.data[0] if isinstance(result.data, list) else result.data

    def update_last_run(self):
        """Stamp last_run_at = now()."""
        self._client.table("crawl_config").update({
            "last_run_at": _now_iso(),
        }).eq("source_name", self._source).execute()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_client_from_env() -> Any:
    """
    Build a supabase.Client from SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY env vars.
    Used by crawler entry points in GitHub Actions.
    """
    import os
    from supabase import create_client
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)
