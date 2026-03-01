"""Unit tests for AkashBackend — no Akash credentials required.

All tests mock out subprocess calls, paramiko, and the akash CLI.
"""

from __future__ import annotations

import base64
import json
import os
import unittest.mock as mock
from unittest.mock import MagicMock, patch

import pytest
import yaml

# ---------------------------------------------------------------------------
# Import helpers from the module under test
# ---------------------------------------------------------------------------
from metaflow_extensions.akash.plugins.backend import (
    _build_sdl,
    _extract_dseq,
    _wait_for_service,
)
from sandrun.backend import Resources, SandboxConfig


# ---------------------------------------------------------------------------
# SDL generation
# ---------------------------------------------------------------------------


class TestBuildSdl:
    """Validate that _build_sdl() produces a well-formed Akash SDL 2.0 YAML."""

    def _config(self, cpu=1, memory_mb=1024, gpu=None, image="python:3.11-slim"):
        return SandboxConfig(
            image=image,
            env={},
            resources=Resources(cpu=cpu, memory_mb=memory_mb, gpu=gpu),
            timeout=600,
        )

    def _parse(self, config, pubkey="ssh-rsa AAAA"):
        raw = _build_sdl(config, pubkey)
        assert raw.startswith("---\n"), "SDL must begin with YAML document separator"
        return yaml.safe_load(raw)

    def test_version_is_string_2_0(self):
        doc = self._parse(self._config())
        assert doc["version"] == "2.0"

    def test_cpu_units_is_float_not_string(self):
        """Regression: cpu.units must be float 1.0, not string '1.0'."""
        doc = self._parse(self._config(cpu=2))
        units = doc["profiles"]["compute"]["worker"]["resources"]["cpu"]["units"]
        assert isinstance(units, float), f"cpu.units must be float, got {type(units)}: {units!r}"
        assert units == 2.0

    def test_cpu_units_minimum_is_1(self):
        doc = self._parse(self._config(cpu=0))
        units = doc["profiles"]["compute"]["worker"]["resources"]["cpu"]["units"]
        assert units >= 1.0

    def test_memory_size_format(self):
        doc = self._parse(self._config(memory_mb=2048))
        size = doc["profiles"]["compute"]["worker"]["resources"]["memory"]["size"]
        assert size == "2048Mi"

    def test_memory_minimum_512(self):
        doc = self._parse(self._config(memory_mb=128))
        size = doc["profiles"]["compute"]["worker"]["resources"]["memory"]["size"]
        assert size == "512Mi"

    def test_storage_present(self):
        doc = self._parse(self._config())
        storage = doc["profiles"]["compute"]["worker"]["resources"]["storage"]
        assert isinstance(storage, list)
        assert len(storage) > 0

    def test_image_propagated(self):
        doc = self._parse(self._config(image="ubuntu:22.04"))
        assert doc["services"]["worker"]["image"] == "ubuntu:22.04"

    def test_ssh_port_exposed(self):
        doc = self._parse(self._config())
        expose = doc["services"]["worker"]["expose"]
        ssh = [e for e in expose if e.get("port") == 22]
        assert ssh, "SSH port 22 must be exposed"
        assert any(p.get("global") for p in ssh[0].get("to", []))

    def test_pubkey_in_env(self):
        pubkey = "ssh-rsa TESTPUBKEY123"
        doc = self._parse(self._config(), pubkey=pubkey)
        env_list = doc["services"]["worker"].get("env", [])
        assert any(pubkey in e for e in env_list), "SSH pubkey must appear in SDL env"

    def test_startup_cmd_is_base64_decode_pipe(self):
        doc = self._parse(self._config())
        args = doc["services"]["worker"]["args"]
        cmd = " ".join(args)
        assert "base64" in cmd, "startup command must decode base64 script"

    def test_startup_script_installs_sshd(self):
        """Verify the embedded startup script includes sshd setup."""
        doc = self._parse(self._config())
        args = doc["services"]["worker"]["args"]
        # args = ["-c", "echo <b64> | base64 -d | bash"]
        # Join and split to get tokens: ["-c", "echo", "<b64>", "|", ...]
        tokens = " ".join(args).split()
        # Find the token after "echo"
        echo_idx = next(i for i, t in enumerate(tokens) if t == "echo")
        b64_token = tokens[echo_idx + 1]
        # Add padding in case length % 4 != 0
        padded = b64_token + "=" * (-len(b64_token) % 4)
        decoded = base64.b64decode(padded).decode(errors="replace")
        assert "sshd" in decoded
        assert "authorized_keys" in decoded

    def test_gpu_section_omitted_when_none(self):
        doc = self._parse(self._config(gpu=None))
        resources = doc["profiles"]["compute"]["worker"]["resources"]
        assert "gpu" not in resources

    def test_gpu_section_present_when_given(self):
        doc = self._parse(self._config(gpu="1"))
        resources = doc["profiles"]["compute"]["worker"]["resources"]
        assert "gpu" in resources
        assert resources["gpu"]["units"] == 1

    def test_deployment_uses_worker_profile(self):
        doc = self._parse(self._config())
        assert doc["deployment"]["worker"]["akash"]["profile"] == "worker"
        assert doc["deployment"]["worker"]["akash"]["count"] == 1

    def test_price_from_env(self, monkeypatch):
        monkeypatch.setenv("METAFLOW_AKASH_SDL_PRICE", "5000")
        doc = self._parse(self._config())
        amount = doc["profiles"]["placement"]["akash"]["pricing"]["worker"]["amount"]
        assert amount == 5000

    def test_missing_yaml_raises_import_error(self, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def no_yaml(name, *args, **kwargs):
            if name == "yaml":
                raise ImportError("No module named 'yaml'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=no_yaml):
            with pytest.raises(ImportError, match="metaflow-akash"):
                _build_sdl(self._config(), "ssh-rsa AAAA")


# ---------------------------------------------------------------------------
# DSEQ extraction
# ---------------------------------------------------------------------------


class TestExtractDseq:
    def test_dseq_in_logs_events(self):
        tx = {
            "logs": [
                {
                    "events": [
                        {
                            "type": "deployment_created",
                            "attributes": [
                                {"key": "dseq", "value": "123456"},
                            ],
                        }
                    ]
                }
            ]
        }
        assert _extract_dseq(tx) == "123456"

    def test_dseq_with_quoted_value(self):
        """dseq value may come wrapped in quotes: "\"123456\""."""
        tx = {
            "logs": [
                {
                    "events": [
                        {
                            "type": "new_deployment",
                            "attributes": [
                                {"key": "dseq", "value": '"789012"'},
                            ],
                        }
                    ]
                }
            ]
        }
        assert _extract_dseq(tx) == "789012"

    def test_dseq_in_top_level_events_plain(self):
        """Cosmos SDK newer format: top-level events with plain string attributes."""
        tx = {
            "events": [
                {
                    "type": "akash.deployment.v1beta3.EventDeploymentCreated",
                    "attributes": [
                        {"key": "dseq", "value": "555000"},
                    ],
                }
            ]
        }
        assert _extract_dseq(tx) == "555000"

    def test_dseq_in_top_level_events_base64(self):
        """Cosmos SDK newer format: top-level events with base64-encoded attributes."""
        key_b64 = base64.b64encode(b"dseq").decode()
        val_b64 = base64.b64encode(b"999888").decode()
        tx = {
            "events": [
                {
                    "type": "akash.deployment.v1beta3.EventDeploymentCreated",
                    "attributes": [
                        {"key": key_b64, "value": val_b64},
                    ],
                }
            ]
        }
        assert _extract_dseq(tx) == "999888"

    def test_missing_dseq_raises(self):
        tx = {"logs": [], "events": []}
        with pytest.raises(RuntimeError, match="Could not extract DSEQ"):
            _extract_dseq(tx)

    def test_non_deployment_event_ignored(self):
        tx = {
            "events": [
                {
                    "type": "coin_received",
                    "attributes": [{"key": "dseq", "value": "111"}],
                },
                {
                    "type": "akash.deployment.v1beta3.EventDeploymentCreated",
                    "attributes": [{"key": "dseq", "value": "222333"}],
                },
            ]
        }
        # Should find the deployment event, not the coin_received one
        assert _extract_dseq(tx) == "222333"

    def test_empty_tx_raises(self):
        with pytest.raises(RuntimeError):
            _extract_dseq({})


# ---------------------------------------------------------------------------
# _wait_for_service: key variant handling
# ---------------------------------------------------------------------------


class TestWaitForService:
    """Verify _wait_for_service handles both forwarded_ports and forwardedPorts."""

    def _make_status(self, key="forwarded_ports"):
        """Build a fake lease-status response using the given key name."""
        return {
            key: {
                "worker": [
                    {
                        "port": 22,
                        "proto": "TCP",
                        "host": "provider.example.com",
                        "externalPort": 32022,
                    }
                ]
            }
        }

    @patch("metaflow_extensions.akash.plugins.backend._get_lease_status")
    def test_snake_case_key(self, mock_status):
        mock_status.return_value = self._make_status("forwarded_ports")
        host, port = _wait_for_service("1234", "1", "1", "provider", timeout=10)
        assert host == "provider.example.com"
        assert port == 32022

    @patch("metaflow_extensions.akash.plugins.backend._get_lease_status")
    def test_camel_case_key(self, mock_status):
        """Regression: Akash CLI may return 'forwardedPorts' (camelCase)."""
        mock_status.return_value = self._make_status("forwardedPorts")
        host, port = _wait_for_service("1234", "1", "1", "provider", timeout=10)
        assert host == "provider.example.com"
        assert port == 32022

    @patch("metaflow_extensions.akash.plugins.backend._get_lease_status")
    def test_timeout_raises(self, mock_status):
        mock_status.return_value = {}
        with pytest.raises(RuntimeError, match="did not expose SSH"):
            _wait_for_service("1234", "1", "1", "provider", timeout=0)

    @patch("metaflow_extensions.akash.plugins.backend._get_lease_status")
    def test_ignores_non_tcp_ports(self, mock_status):
        mock_status.return_value = {
            "forwarded_ports": {
                "worker": [
                    {"port": 22, "proto": "UDP", "host": "h.example.com", "externalPort": 22},
                    {"port": 22, "proto": "TCP", "host": "h.example.com", "externalPort": 30022},
                ]
            }
        }
        host, port = _wait_for_service("1234", "1", "1", "provider", timeout=10)
        assert port == 30022

    @patch("metaflow_extensions.akash.plugins.backend._get_lease_status")
    def test_ignores_non_ssh_ports(self, mock_status):
        mock_status.return_value = {
            "forwarded_ports": {
                "worker": [
                    {"port": 80, "proto": "TCP", "host": "h.example.com", "externalPort": 30080},
                ]
            }
        }
        with pytest.raises(RuntimeError, match="did not expose SSH"):
            _wait_for_service("1234", "1", "1", "provider", timeout=0)


# ---------------------------------------------------------------------------
# SSH retry logic
# ---------------------------------------------------------------------------


class TestGetSsh:
    """Verify _get_ssh retries on connection failure."""

    def _make_backend_with_state(self, mock_paramiko, host="host", port=22):
        """Create an AkashBackend with a pre-populated deployment state."""
        from metaflow_extensions.akash.plugins.backend import AkashBackend, _DeploymentState, _LeaseId

        backend = object.__new__(AkashBackend)
        backend._deployments = {}

        # Use a MagicMock as the SSH key — _DeploymentState accepts Any
        fake_key = MagicMock()
        state = _DeploymentState(
            lease=_LeaseId(dseq="1234", gseq="1", oseq="1", provider="p", owner="o"),
            ssh_host=host,
            ssh_port=port,
            ssh_key=fake_key,
            sdl_path="/tmp/fake.yaml",
        )
        backend._deployments["1234"] = state
        return backend

    def _make_mock_paramiko(self):
        """Return a mock paramiko module for sys.modules injection."""
        mock_paramiko = MagicMock()
        mock_paramiko.AutoAddPolicy = MagicMock
        return mock_paramiko

    def test_success_on_first_attempt(self):
        mock_paramiko = self._make_mock_paramiko()
        mock_client = MagicMock()
        mock_paramiko.SSHClient.return_value = mock_client

        with patch.dict("sys.modules", {"paramiko": mock_paramiko}):
            backend = self._make_backend_with_state(mock_paramiko)
            result = backend._get_ssh("1234")

        assert result is mock_client
        mock_client.connect.assert_called_once()

    @patch("time.sleep")
    def test_retries_on_connection_error(self, mock_sleep):
        """Should retry up to 15 times, succeed on attempt 3."""
        mock_paramiko = self._make_mock_paramiko()
        mock_client = MagicMock()
        mock_paramiko.SSHClient.return_value = mock_client
        mock_client.connect.side_effect = [
            OSError("Connection refused"),
            OSError("Connection refused"),
            None,  # succeed on 3rd attempt
        ]

        with patch.dict("sys.modules", {"paramiko": mock_paramiko}):
            backend = self._make_backend_with_state(mock_paramiko)
            result = backend._get_ssh("1234")

        assert result is mock_client
        assert mock_client.connect.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("time.sleep")
    def test_raises_after_max_retries(self, mock_sleep):
        mock_paramiko = self._make_mock_paramiko()
        mock_client = MagicMock()
        mock_paramiko.SSHClient.return_value = mock_client
        mock_client.connect.side_effect = OSError("Connection refused")

        with patch.dict("sys.modules", {"paramiko": mock_paramiko}):
            backend = self._make_backend_with_state(mock_paramiko)
            with pytest.raises(RuntimeError, match="SSH connection.*failed after"):
                backend._get_ssh("1234")

        assert mock_client.connect.call_count == 15

    def test_returns_cached_active_transport(self):
        """Should not reconnect if transport is already active."""
        mock_paramiko = self._make_mock_paramiko()
        mock_client = MagicMock()
        mock_transport = MagicMock()
        mock_transport.is_active.return_value = True
        mock_client.get_transport.return_value = mock_transport

        with patch.dict("sys.modules", {"paramiko": mock_paramiko}):
            backend = self._make_backend_with_state(mock_paramiko)
            backend._deployments["1234"]._ssh_client = mock_client
            result = backend._get_ssh("1234")

        assert result is mock_client
        mock_paramiko.SSHClient.assert_not_called()

    def test_reconnects_when_transport_dead(self):
        """Should reconnect if existing transport is not active."""
        mock_paramiko = self._make_mock_paramiko()

        stale_client = MagicMock()
        stale_transport = MagicMock()
        stale_transport.is_active.return_value = False
        stale_client.get_transport.return_value = stale_transport

        fresh_client = MagicMock()
        mock_paramiko.SSHClient.return_value = fresh_client

        with patch.dict("sys.modules", {"paramiko": mock_paramiko}):
            backend = self._make_backend_with_state(mock_paramiko)
            backend._deployments["1234"]._ssh_client = stale_client
            result = backend._get_ssh("1234")

        assert result is fresh_client
        fresh_client.connect.assert_called_once()


# ---------------------------------------------------------------------------
# Streaming sleep optimization
# ---------------------------------------------------------------------------


class TestExecStrSleep:
    """Verify that the read loop only sleeps when no data is available."""

    def _make_backend(self):
        from metaflow_extensions.akash.plugins.backend import AkashBackend, _DeploymentState, _LeaseId

        backend = object.__new__(AkashBackend)
        backend._deployments = {}

        state = _DeploymentState(
            lease=_LeaseId(dseq="1234", gseq="1", oseq="1", provider="p", owner="o"),
            ssh_host="h",
            ssh_port=22,
            ssh_key=MagicMock(),
            sdl_path="/tmp/fake.yaml",
        )
        backend._deployments["1234"] = state
        return backend, state

    @patch("time.sleep")
    def test_no_sleep_when_data_flows(self, mock_sleep):
        """When stdout/stderr data is immediately available, sleep should be minimal."""
        backend, state = self._make_backend()

        # Build a mock SSH client+transport+channel
        mock_channel = MagicMock()
        # Simulate: data on first two recv_ready calls, then nothing, then exit
        stdout_calls = iter([True, True, False, False, False])
        stderr_calls = iter([False, False, False, False])

        def recv_ready():
            try:
                return next(stdout_calls)
            except StopIteration:
                return False

        def recv_stderr_ready():
            try:
                return next(stderr_calls)
            except StopIteration:
                return False

        exit_ready_calls = iter([False, False, True])

        def exit_status_ready():
            try:
                return next(exit_ready_calls)
            except StopIteration:
                return True

        mock_channel.recv_ready.side_effect = recv_ready
        mock_channel.recv_stderr_ready.side_effect = recv_stderr_ready
        mock_channel.recv.return_value = b"hello\n"
        mock_channel.recv_stderr.return_value = b""
        mock_channel.exit_status_ready.side_effect = exit_status_ready
        mock_channel.recv_exit_status.return_value = 0

        mock_transport = MagicMock()
        mock_transport.open_session.return_value = mock_channel

        mock_ssh = MagicMock()
        mock_ssh.get_transport.return_value = mock_transport

        with patch.object(backend, "_get_ssh", return_value=mock_ssh):
            backend._exec_str("1234", "echo hello", timeout=30)

        # In iterations where data was read, no sleep should have been called.
        # In iterations where no data was available (before exit_status_ready),
        # we may sleep at most once (the one idle loop before exit is detected).
        assert mock_sleep.call_count <= 2, (
            f"Expected minimal sleep calls when data is available, "
            f"got {mock_sleep.call_count}"
        )
