"""Unit tests for the discovery module."""

from unittest.mock import MagicMock

from src.discovery import resolve_dataset_registry


class TestResolveDatasetRegistry:
    def _make_client(self, api_datasets=None):
        client = MagicMock()
        client.get_datasets_in_workspace_safe.return_value = api_datasets or []
        return client

    def test_explicit_datasets_only(self):
        config = {
            "datasets": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "dataset_id": "ds-1",
                    "dataset_name": "Dataset 1",
                    "is_critical": True,
                    "is_active": True,
                }
            ]
        }
        client = self._make_client()
        result = resolve_dataset_registry(client, config, inter_request_delay=0)
        assert len(result) == 1
        assert result[0]["dataset_id"] == "ds-1"
        assert result[0]["is_critical"] is True
        client.get_datasets_in_workspace_safe.assert_not_called()

    def test_inactive_datasets_excluded(self):
        config = {
            "datasets": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "dataset_id": "ds-1",
                    "dataset_name": "Active",
                    "is_active": True,
                },
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "dataset_id": "ds-2",
                    "dataset_name": "Inactive",
                    "is_active": False,
                },
            ]
        }
        client = self._make_client()
        result = resolve_dataset_registry(client, config, inter_request_delay=0)
        assert len(result) == 1
        assert result[0]["dataset_id"] == "ds-1"

    def test_workspace_discovery(self):
        config = {
            "datasets": [],
            "workspaces": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "is_critical_default": False,
                    "is_active": True,
                    "exclude_datasets": [],
                }
            ],
        }
        api_datasets = [
            {"id": "ds-a", "name": "Dataset A"},
            {"id": "ds-b", "name": "Dataset B"},
        ]
        client = self._make_client(api_datasets)
        result = resolve_dataset_registry(client, config, inter_request_delay=0)
        assert len(result) == 2
        ids = {r["dataset_id"] for r in result}
        assert ids == {"ds-a", "ds-b"}

    def test_workspace_exclude_datasets(self):
        config = {
            "datasets": [],
            "workspaces": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "is_critical_default": False,
                    "is_active": True,
                    "exclude_datasets": ["ds-b"],
                }
            ],
        }
        api_datasets = [
            {"id": "ds-a", "name": "Dataset A"},
            {"id": "ds-b", "name": "Dataset B"},
        ]
        client = self._make_client(api_datasets)
        result = resolve_dataset_registry(client, config, inter_request_delay=0)
        assert len(result) == 1
        assert result[0]["dataset_id"] == "ds-a"

    def test_explicit_takes_precedence_over_discovered(self):
        config = {
            "datasets": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "dataset_id": "ds-a",
                    "dataset_name": "Explicit Name",
                    "is_critical": True,
                    "is_active": True,
                }
            ],
            "workspaces": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "is_critical_default": False,
                    "is_active": True,
                    "exclude_datasets": [],
                }
            ],
        }
        api_datasets = [
            {"id": "ds-a", "name": "API Name"},
            {"id": "ds-b", "name": "Dataset B"},
        ]
        client = self._make_client(api_datasets)
        result = resolve_dataset_registry(client, config, inter_request_delay=0)
        assert len(result) == 2
        by_id = {r["dataset_id"]: r for r in result}
        assert by_id["ds-a"]["dataset_name"] == "Explicit Name"
        assert by_id["ds-a"]["is_critical"] is True
        assert by_id["ds-b"]["dataset_name"] == "Dataset B"

    def test_inactive_workspace_skipped(self):
        config = {
            "datasets": [],
            "workspaces": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "is_critical_default": False,
                    "is_active": False,
                    "exclude_datasets": [],
                }
            ],
        }
        client = self._make_client([{"id": "ds-a", "name": "A"}])
        result = resolve_dataset_registry(client, config, inter_request_delay=0)
        assert len(result) == 0
        client.get_datasets_in_workspace_safe.assert_not_called()

    def test_empty_config(self):
        client = self._make_client()
        result = resolve_dataset_registry(client, {}, inter_request_delay=0)
        assert result == []

    def test_is_critical_default_applied(self):
        config = {
            "datasets": [],
            "workspaces": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "is_critical_default": True,
                    "is_active": True,
                    "exclude_datasets": [],
                }
            ],
        }
        client = self._make_client([{"id": "ds-a", "name": "A"}])
        result = resolve_dataset_registry(client, config, inter_request_delay=0)
        assert result[0]["is_critical"] is True

    def test_multiple_workspaces(self):
        config = {
            "datasets": [],
            "workspaces": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "is_critical_default": False,
                    "is_active": True,
                    "exclude_datasets": [],
                },
                {
                    "workspace_id": "ws-2",
                    "workspace_name": "WS 2",
                    "is_critical_default": True,
                    "is_active": True,
                    "exclude_datasets": [],
                },
            ],
        }
        client = MagicMock()
        client.get_datasets_in_workspace_safe.side_effect = [
            [{"id": "ds-1", "name": "D1"}],
            [{"id": "ds-2", "name": "D2"}],
        ]
        result = resolve_dataset_registry(client, config, inter_request_delay=0)
        assert len(result) == 2
        by_id = {r["dataset_id"]: r for r in result}
        assert by_id["ds-1"]["workspace_id"] == "ws-1"
        assert by_id["ds-2"]["workspace_id"] == "ws-2"
        assert by_id["ds-2"]["is_critical"] is True

    def test_dedup_across_workspaces(self):
        config = {
            "datasets": [],
            "workspaces": [
                {
                    "workspace_id": "ws-1",
                    "workspace_name": "WS 1",
                    "is_critical_default": False,
                    "is_active": True,
                    "exclude_datasets": [],
                },
                {
                    "workspace_id": "ws-2",
                    "workspace_name": "WS 2",
                    "is_critical_default": True,
                    "is_active": True,
                    "exclude_datasets": [],
                },
            ],
        }
        client = MagicMock()
        client.get_datasets_in_workspace_safe.side_effect = [
            [{"id": "ds-shared", "name": "Shared"}],
            [{"id": "ds-shared", "name": "Shared Copy"}],
        ]
        result = resolve_dataset_registry(client, config, inter_request_delay=0)
        assert len(result) == 1
        assert result[0]["workspace_id"] == "ws-1"
