"""Unit tests for the query_probe module."""

from unittest.mock import MagicMock

import requests

from src.query_probe import get_active_probes, measure_query


def _probe(**overrides):
    base = {
        "workspace_id": "ws-1",
        "workspace_name": "WS 1",
        "dataset_id": "ds-1",
        "dataset_name": "Dataset 1",
        "query_label": "baseline",
        "dax_query": 'EVALUATE ROW("x", 1)',
        "is_critical": True,
        "is_active": True,
    }
    base.update(overrides)
    return base


class TestMeasureQuery:
    def test_success_record(self):
        client = MagicMock()
        client.execute_dax_query.return_value = {"duration_ms": 123.4, "row_count": 5}

        record = measure_query(client, _probe())

        assert record["status"] == "Success"
        assert record["duration_ms"] == 123.4
        assert record["row_count"] == 5
        assert record["error_message"] is None
        assert record["dataset_id"] == "ds-1"
        assert record["query_label"] == "baseline"
        assert record["is_critical"] is True
        assert record["probe_id"]
        assert record["probe_timestamp"]
        assert record["probe_date"]

    def test_unique_probe_ids(self):
        client = MagicMock()
        client.execute_dax_query.return_value = {"duration_ms": 1.0, "row_count": 0}
        r1 = measure_query(client, _probe())
        r2 = measure_query(client, _probe())
        assert r1["probe_id"] != r2["probe_id"]

    def test_http_error_recorded_as_failed(self):
        client = MagicMock()
        response = MagicMock()
        response.status_code = 403
        client.execute_dax_query.side_effect = requests.HTTPError(response=response)

        record = measure_query(client, _probe())

        assert record["status"] == "Failed"
        assert record["duration_ms"] is None
        assert record["row_count"] is None
        assert record["error_message"] == "HTTP 403"

    def test_generic_error_recorded_as_failed(self):
        client = MagicMock()
        client.execute_dax_query.side_effect = ValueError("boom")

        record = measure_query(client, _probe())

        assert record["status"] == "Failed"
        assert "boom" in record["error_message"]

    def test_does_not_raise_on_failure(self):
        client = MagicMock()
        client.execute_dax_query.side_effect = RuntimeError("network down")
        # Should not raise.
        record = measure_query(client, _probe())
        assert record["status"] == "Failed"

    def test_defaults_when_optional_fields_missing(self):
        client = MagicMock()
        client.execute_dax_query.return_value = {"duration_ms": 10.0, "row_count": 1}
        probe = {
            "workspace_id": "ws-1",
            "dataset_id": "ds-1",
            "dax_query": "EVALUATE ROW(\"x\", 1)",
        }
        record = measure_query(client, probe)
        assert record["is_critical"] is False
        assert record["workspace_name"] is None
        assert record["query_label"] is None


class TestGetActiveProbes:
    def test_filters_inactive(self):
        config = {
            "probes": [
                _probe(dataset_id="a", is_active=True),
                _probe(dataset_id="b", is_active=False),
                _probe(dataset_id="c"),
            ]
        }
        result = get_active_probes(config)
        ids = {p["dataset_id"] for p in result}
        assert ids == {"a", "c"}

    def test_empty_config(self):
        assert get_active_probes({}) == []
