"""Tests for crawlers/base.py"""
import time
from unittest.mock import MagicMock, patch, call
import pytest
from crawlers.base import SupabaseWriter, CrawlLogger, CrawlConfig, retry_with_backoff


# ---------------------------------------------------------------------------
# retry_with_backoff
# ---------------------------------------------------------------------------

def test_retry_succeeds_on_first_attempt():
    fn = MagicMock(return_value="ok")
    result = retry_with_backoff(fn, max_attempts=3)
    assert result == "ok"
    assert fn.call_count == 1


def test_retry_succeeds_on_third_attempt():
    fn = MagicMock(side_effect=[RuntimeError("fail"), RuntimeError("fail"), "ok"])
    with patch("time.sleep"):  # don't actually sleep in tests
        result = retry_with_backoff(fn, max_attempts=3, base_delay=0.01)
    assert result == "ok"
    assert fn.call_count == 3


def test_retry_raises_after_max_attempts():
    fn = MagicMock(side_effect=RuntimeError("always fails"))
    with patch("time.sleep"):
        with pytest.raises(RuntimeError, match="always fails"):
            retry_with_backoff(fn, max_attempts=3, base_delay=0.01)
    assert fn.call_count == 3


# ---------------------------------------------------------------------------
# SupabaseWriter
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_supabase_client():
    """A MagicMock standing in for the supabase.Client."""
    client = MagicMock()
    execute_result = MagicMock()
    execute_result.data = [{"id": 1}, {"id": 2}]
    client.table.return_value.upsert.return_value.execute.return_value = execute_result
    client.rpc.return_value.execute.return_value = MagicMock(data=None)
    return client


def test_supabase_writer_upsert_returns_count(mock_supabase_client):
    writer = SupabaseWriter(mock_supabase_client)
    records = [{"station_id": 1, "recorded_at": "2024-01-01T00:00:00Z", "level_m": 1.5}]
    count = writer.upsert("water_levels", records, on_conflict="station_id,recorded_at")
    assert count == 2  # len of execute_result.data
    mock_supabase_client.table.assert_called_with("water_levels")


def test_supabase_writer_upsert_empty_list_is_noop(mock_supabase_client):
    writer = SupabaseWriter(mock_supabase_client)
    count = writer.upsert("water_levels", [], on_conflict="station_id,recorded_at")
    assert count == 0
    mock_supabase_client.table.assert_not_called()


def test_supabase_writer_truncate_and_insert(mock_supabase_client):
    writer = SupabaseWriter(mock_supabase_client)
    records = [{"ward_code": "VN001", "severity": "high"}]
    execute_result = MagicMock()
    execute_result.data = records
    mock_supabase_client.table.return_value.insert.return_value.execute.return_value = execute_result

    count = writer.truncate_and_insert("flood_warnings", records)
    mock_supabase_client.rpc.assert_called_once_with("truncate_table", {"table_name": "flood_warnings"})
    assert count == 1


# ---------------------------------------------------------------------------
# CrawlLogger
# ---------------------------------------------------------------------------

def test_crawl_logger_start_inserts_running_row(mock_supabase_client):
    insert_result = MagicMock()
    insert_result.data = [{"id": 42}]
    mock_supabase_client.table.return_value.insert.return_value.execute.return_value = insert_result

    logger = CrawlLogger(mock_supabase_client, "jtwc")
    log_id = logger.start()
    assert log_id == 42


def test_crawl_logger_finish_updates_row(mock_supabase_client):
    update_result = MagicMock()
    update_result.data = [{"id": 42}]
    mock_supabase_client.table.return_value.update.return_value.eq.return_value.execute.return_value = update_result

    logger = CrawlLogger(mock_supabase_client, "jtwc")
    logger.finish(log_id=42, records_upserted=100, status="success")
    mock_supabase_client.table.assert_called_with("crawl_log")


# ---------------------------------------------------------------------------
# CrawlConfig
# ---------------------------------------------------------------------------

def test_crawl_config_get_returns_row(mock_supabase_client):
    row = {
        "source_name": "jtwc",
        "normal_interval_min": 60,
        "alert_interval_min": 30,
        "is_alert_mode": False,
    }
    select_result = MagicMock()
    select_result.data = [row]
    mock_supabase_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = select_result

    config = CrawlConfig(mock_supabase_client, "jtwc")
    result = config.get()
    assert result["source_name"] == "jtwc"
    assert result["is_alert_mode"] is False


def test_crawl_config_update_last_run(mock_supabase_client):
    update_result = MagicMock()
    mock_supabase_client.table.return_value.update.return_value.eq.return_value.execute.return_value = update_result

    config = CrawlConfig(mock_supabase_client, "jtwc")
    config.update_last_run()
    mock_supabase_client.table.assert_called_with("crawl_config")
