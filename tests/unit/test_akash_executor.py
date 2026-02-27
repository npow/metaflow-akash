"""Unit tests for AkashExecutor — no Akash credentials or network required."""

from __future__ import annotations

import os
import shlex
import unittest.mock as mock
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Stub out metaflow imports so tests work standalone
# ---------------------------------------------------------------------------

MFLOG_STDOUT_EXPORT = "export MFLOG_STDOUT=/tmp/mflog_stdout"
BASH_SAVE_LOGS_STUB = "echo save_logs"
BASH_CAPTURE_LOGS_WRAP = lambda cmd: f"( {cmd} )"  # noqa: E731

import sys

metaflow_stubs = {
    "metaflow": MagicMock(),
    "metaflow.util": MagicMock(),
    "metaflow.exception": MagicMock(),
    "metaflow.metaflow_config": MagicMock(
        DEFAULT_METADATA="local",
        SERVICE_INTERNAL_URL="",
    ),
    "metaflow.mflog": MagicMock(
        BASH_SAVE_LOGS=BASH_SAVE_LOGS_STUB,
        bash_capture_logs=BASH_CAPTURE_LOGS_WRAP,
        export_mflog_env_vars=lambda **kw: MFLOG_STDOUT_EXPORT,
    ),
    "metaflow.metaflow_config_funcs": MagicMock(config_values=lambda: []),
}

for mod, stub in metaflow_stubs.items():
    sys.modules.setdefault(mod, stub)

# Stub sandrun so backend.py can be imported without akash extras installed.
_sandrun_stub = MagicMock()
_sandrun_backend_stub = MagicMock()
_sandrun_backends_stub = MagicMock()
_sandrun_backends_akash_stub = MagicMock()
_sandrun_backends_akash_stub.AkashBackend = MagicMock

for _mod, _stub in [
    ("sandrun", _sandrun_stub),
    ("sandrun.backend", _sandrun_backend_stub),
    ("sandrun.backends", _sandrun_backends_stub),
    ("sandrun.backends.akash", _sandrun_backends_akash_stub),
]:
    sys.modules.setdefault(_mod, _stub)

# Also stub metaflow.util.get_username
sys.modules["metaflow"].util = MagicMock()
sys.modules["metaflow"].util.get_username.return_value = "testuser"
sys.modules["metaflow.util"].get_username = MagicMock(return_value="testuser")


# ---------------------------------------------------------------------------
# Helpers for building a minimal AkashExecutor
# ---------------------------------------------------------------------------

def _make_executor(stager=None, installer=None):
    """Build AkashExecutor with a stubbed environment."""
    # Lazy import after stubs are installed
    from metaflow_extensions.akash.plugins.akash_executor import AkashExecutor

    env = MagicMock()
    env.get_package_commands.return_value = ["echo download_pkg"]
    env.bootstrap_commands.return_value = []
    env.executable.return_value = "/usr/bin/python3"

    return AkashExecutor(env, stager=stager, installer=installer)


# ---------------------------------------------------------------------------
# _command(): verify env vars survive shlex.quote round-trip
# ---------------------------------------------------------------------------

class TestCommand:
    def _run_command(self, sandbox_env: dict) -> str:
        executor = _make_executor()
        return executor._command(
            code_package_metadata="sha256:abc",
            code_package_url="s3://bucket/code.tar",
            step_name="my_step",
            step_cmds=["echo run_step"],
            task_spec={
                "flow_name": "TestFlow",
                "step_name": "my_step",
                "run_id": "1",
                "task_id": "1",
                "retry_count": "0",
            },
            datastore_type="s3",
            sandbox_env=sandbox_env,
        )

    def test_simple_value_survives(self):
        env = {"SIMPLE_VAR": "hello_world"}
        cmd = self._run_command(env)
        assert "SIMPLE_VAR" in cmd
        assert "hello_world" in cmd

    def test_value_with_spaces_survives(self):
        env = {"MY_VAR": "hello world with spaces"}
        cmd = self._run_command(env)
        # shlex.quote wraps in single quotes
        assert "hello world with spaces" in cmd

    def test_json_value_survives(self):
        """Critical regression: JSON values must not be mangled by any quoting."""
        json_val = '{"key": "value", "nested": {"a": 1}}'
        env = {"METAFLOW_CODE_METADATA": json_val}
        cmd = self._run_command(env)
        # The JSON string must appear verbatim in the script
        assert json_val in cmd, (
            f"JSON value was mangled.\nOriginal: {json_val!r}\nCommand snippet: {cmd[:500]}"
        )

    def test_s3_url_survives(self):
        """S3 URLs with query strings and slashes must survive quoting."""
        url = "s3://my-bucket/path/to/code.tar?version=abc&encoding=utf8"
        env = {"METAFLOW_CODE_URL": url}
        cmd = self._run_command(env)
        assert url in cmd, f"S3 URL was mangled.\nOriginal: {url!r}"

    def test_env_vars_sorted(self):
        env = {"ZZZ": "last", "AAA": "first", "MMM": "middle"}
        cmd = self._run_command(env)
        aaa_pos = cmd.find("AAA")
        mmm_pos = cmd.find("MMM")
        zzz_pos = cmd.find("ZZZ")
        assert aaa_pos < mmm_pos < zzz_pos, "env vars must be sorted alphabetically"

    def test_none_values_excluded(self):
        env = {"PRESENT": "yes", "ABSENT": None}
        cmd = self._run_command(env)
        assert "PRESENT" in cmd
        assert "ABSENT" not in cmd

    def test_export_keyword_present(self):
        env = {"FOO": "bar"}
        cmd = self._run_command(env)
        assert "export" in cmd

    def test_mkdir_logs_dir(self):
        cmd = self._run_command({})
        assert "mkdir -p" in cmd
        assert ".logs" in cmd

    def test_bash_save_logs_present(self):
        cmd = self._run_command({})
        assert BASH_SAVE_LOGS_STUB in cmd

    def test_step_cmd_present(self):
        executor = _make_executor()
        cmd = executor._command(
            code_package_metadata="sha256:abc",
            code_package_url="s3://bucket/code.tar",
            step_name="my_step",
            step_cmds=["python flow.py step my_step --run-id 1"],
            task_spec={
                "flow_name": "F", "step_name": "my_step",
                "run_id": "1", "task_id": "1", "retry_count": "0",
            },
            datastore_type="s3",
            sandbox_env={},
        )
        assert "python flow.py step my_step" in cmd

    def test_stager_setup_commands_used(self):
        stager = MagicMock()
        stager.setup_commands.return_value = ["echo stager_setup"]
        executor = _make_executor(stager=stager)

        cmd = executor._command(
            code_package_metadata="sha256:abc",
            code_package_url="s3://bucket/code.tar",
            step_name="s",
            step_cmds=["echo step"],
            task_spec={"flow_name": "F", "step_name": "s", "run_id": "1",
                       "task_id": "1", "retry_count": "0"},
            datastore_type="s3",
            sandbox_env={},
        )
        assert "stager_setup" in cmd
        # environment.get_package_commands must NOT be called
        executor._environment.get_package_commands.assert_not_called()

    def test_installer_setup_commands_used(self):
        installer = MagicMock()
        installer.setup_commands.return_value = ["echo installer_setup"]
        executor = _make_executor(installer=installer)

        cmd = executor._command(
            code_package_metadata="sha256:abc",
            code_package_url="s3://bucket/code.tar",
            step_name="s",
            step_cmds=["echo step"],
            task_spec={"flow_name": "F", "step_name": "s", "run_id": "1",
                       "task_id": "1", "retry_count": "0"},
            datastore_type="s3",
            sandbox_env={},
        )
        assert "installer_setup" in cmd
        executor._environment.bootstrap_commands.assert_not_called()


# ---------------------------------------------------------------------------
# _build_env(): verify required keys are present
# ---------------------------------------------------------------------------

class TestBuildEnv:
    def _build(self, **overrides):
        from metaflow_extensions.akash.plugins.akash_executor import AkashExecutor

        return AkashExecutor._build_env(
            code_package_metadata="sha256:abc",
            code_package_sha="deadbeef",
            code_package_url="s3://bucket/code.tar",
            datastore_type="s3",
            sandbox_id="12345",
            sandbox_env_override=overrides or None,
        )

    def test_metaflow_code_metadata(self):
        env = self._build()
        assert env["METAFLOW_CODE_METADATA"] == "sha256:abc"

    def test_metaflow_code_sha(self):
        env = self._build()
        assert env["METAFLOW_CODE_SHA"] == "deadbeef"

    def test_metaflow_code_url(self):
        env = self._build()
        assert env["METAFLOW_CODE_URL"] == "s3://bucket/code.tar"

    def test_metaflow_code_ds(self):
        env = self._build()
        assert env["METAFLOW_CODE_DS"] == "s3"

    def test_metaflow_default_datastore(self):
        env = self._build()
        assert env["METAFLOW_DEFAULT_DATASTORE"] == "s3"

    def test_metaflow_akash_workload_flag(self):
        env = self._build()
        assert env["METAFLOW_AKASH_WORKLOAD"] == "1"

    def test_metaflow_akash_dseq(self):
        env = self._build()
        assert env["METAFLOW_AKASH_DSEQ"] == "12345"

    def test_override_takes_precedence(self):
        env = self._build(METAFLOW_CODE_DS="azure")
        assert env["METAFLOW_CODE_DS"] == "azure"

    def test_aws_creds_forwarded(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA_TEST")
        env = self._build()
        assert env.get("AWS_ACCESS_KEY_ID") == "AKIA_TEST"

    def test_absent_aws_creds_not_forwarded(self, monkeypatch):
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        env = self._build()
        assert "AWS_ACCESS_KEY_ID" not in env

    def test_service_metadata_proxied_as_local(self):
        """When DEFAULT_METADATA=='service', container should use 'local'."""
        import metaflow.metaflow_config as cfg

        original = cfg.DEFAULT_METADATA
        try:
            cfg.DEFAULT_METADATA = "service"
            from metaflow_extensions.akash.plugins.akash_executor import AkashExecutor

            env = AkashExecutor._build_env(
                code_package_metadata="x",
                code_package_sha="x",
                code_package_url="x",
                datastore_type="s3",
                sandbox_id="1",
            )
            assert env["METAFLOW_DEFAULT_METADATA"] == "local"
        finally:
            cfg.DEFAULT_METADATA = original


# ---------------------------------------------------------------------------
# Script correctness: the generated script can be parsed by bash -n
# ---------------------------------------------------------------------------

class TestScriptSyntax:
    """Run 'bash -n' (syntax check) on the generated step script."""

    def test_generated_script_passes_bash_syntax_check(self, tmp_path):
        import subprocess

        env_vars = {
            "METAFLOW_CODE_URL": "s3://bucket/code.tar?v=1&enc=utf8",
            "METAFLOW_CODE_METADATA": '{"key": "val", "nested": {"a": 1}}',
            "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
            "SIMPLE": "hello world",
        }
        executor = _make_executor()
        cmd = executor._command(
            code_package_metadata='{"key": "val"}',
            code_package_url="s3://bucket/code.tar",
            step_name="start",
            step_cmds=["python flow.py step start --run-id 1 --task-id 1"],
            task_spec={
                "flow_name": "TestFlow",
                "step_name": "start",
                "run_id": "1",
                "task_id": "1",
                "retry_count": "0",
            },
            datastore_type="s3",
            sandbox_env=env_vars,
        )
        script_path = tmp_path / "step.sh"
        script_path.write_text(cmd)

        result = subprocess.run(
            ["bash", "-n", str(script_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"bash -n syntax check failed:\n"
            f"stderr: {result.stderr}\n"
            f"Script:\n{cmd[:2000]}"
        )
