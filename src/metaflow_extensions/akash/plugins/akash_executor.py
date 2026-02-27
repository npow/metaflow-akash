"""Akash executor — Metaflow integration layer for AkashBackend.

Layer: Execution (same level as Metaflow Integration)
May only import from: .backend, metaflow stdlib, sandrun

This module is the Metaflow-specific wrapper around AkashBackend.
It handles Metaflow-specific concerns:

- mflog structured log capture (export_mflog_env_vars, bash_capture_logs, BASH_SAVE_LOGS)
- Metaflow environment variable assembly (DEFAULT_METADATA, config_values, credentials)
- Injectable PackageStager / DepInstaller for backend-native code+dep delivery
- Backward-compatible fallback to environment.get_package_commands() / bootstrap_commands()

Mirrors ``metaflow.plugins.aws.batch.batch.Batch`` in structure.
"""

from __future__ import annotations

import os
import shlex
import sys
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

from metaflow import util
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import DEFAULT_METADATA
from metaflow.metaflow_config import SERVICE_INTERNAL_URL
from metaflow.mflog import BASH_SAVE_LOGS
from metaflow.mflog import bash_capture_logs
from metaflow.mflog import export_mflog_env_vars

from .backend import AkashBackend

if TYPE_CHECKING:
    from sandrun.backend import ExecResult

LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)

# Cloud credential env vars to forward into the Akash container.
_FORWARDED_CREDENTIAL_VARS = [
    # AWS
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_DEFAULT_REGION",
    # GCS
    "GOOGLE_APPLICATION_CREDENTIALS",
    "CLOUDSDK_CONFIG",
    # Azure
    "AZURE_STORAGE_CONNECTION_STRING",
    "AZURE_STORAGE_KEY",
]

_STAGING_BIN_DIR = "/tmp/metaflow-akash/bin"
_DEFAULT_DEBUG_DIR = "/tmp/metaflow-akash-debug"


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes", "on")


def _debug_settings() -> tuple[bool, str | None]:
    """Return (keep_deployment, dump_script_dir)."""
    cfg = os.environ.get("METAFLOW_AKASH_DEBUG", "").strip()
    if not cfg or cfg.lower() in ("0", "false", "no", "off"):
        return False, None
    if cfg.lower() in ("1", "true", "yes", "on"):
        return True, _DEFAULT_DEBUG_DIR
    return True, cfg


class AkashException(MetaflowException):
    headline = "Akash execution error"


class AkashExecutor:
    """Create an Akash deployment, ship code, execute a Metaflow step inside it.

    Env vars are injected inline in the step script (not via SDL) so that
    complex values (S3 URLs, JSON blobs) don't require YAML-safe encoding
    in the deployment manifest.

    Stager / installer injection
    ----------------------------
    Pass a ``sandrun.PackageStager`` to deliver the code package via
    ``backend.upload()`` instead of downloading from the remote datastore.
    Pass a ``sandrun.DepInstaller`` to install dependencies offline.
    When neither is provided, the executor falls back to the classic
    ``environment.get_package_commands()`` / ``bootstrap_commands()`` paths.
    """

    def __init__(
        self,
        environment: Any,
        stager: Any | None = None,
        installer: Any | None = None,
    ) -> None:
        self._environment = environment
        self._stager = stager
        self._installer = installer
        self._backend: AkashBackend | None = None
        self._sandbox_id: str | None = None
        self._log_streamed = False
        self._result: ExecResult | None = None

    # ------------------------------------------------------------------
    # Command building
    # ------------------------------------------------------------------

    def _command(
        self,
        code_package_metadata: str,
        code_package_url: str,
        step_name: str,
        step_cmds: list[str],
        task_spec: dict[str, str],
        datastore_type: str,
        sandbox_env: dict[str, str],
    ) -> str:
        """Build the full bash script that runs inside the Akash container.

        Env vars are exported inline at the top of the script rather than
        relying on the SDL env section, which avoids YAML quoting issues
        for complex values (URLs with query strings, JSON, etc.).
        """
        mflog_expr = export_mflog_env_vars(
            datastore_type=datastore_type,
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
            **task_spec,
        )

        # Inline env exports (shlex.quote handles all special characters).
        env_exports = " && ".join(
            f"export {shlex.quote(k)}={shlex.quote(str(v))}"
            for k, v in sorted(sandbox_env.items())
            if v is not None
        )

        # Code package setup
        if self._stager is not None:
            init_cmds = self._stager.setup_commands()
        else:
            init_cmds = self._environment.get_package_commands(
                code_package_url, datastore_type, code_package_metadata
            )
            init_cmds = [
                cmd.replace(
                    "mkdir metaflow && cd metaflow",
                    "mkdir mf_akash && cd mf_akash",
                )
                for cmd in init_cmds
            ]
        init_expr = " && ".join(init_cmds)

        # Dependency bootstrap
        if self._installer is not None:
            bootstrap_cmds = self._installer.setup_commands()
        else:
            bootstrap_cmds = self._environment.bootstrap_commands(
                step_name, datastore_type
            )

        step_expr = bash_capture_logs(" && ".join(bootstrap_cmds + step_cmds))

        cmd_str = (
            f"true"
            f" && {env_exports}"
            f" && mkdir -p {LOGS_DIR}"
            f" && {mflog_expr}"
            f" && {init_expr}"
            f" && {step_expr}"
            f"; c=$?; {BASH_SAVE_LOGS}; exit $c"
        )
        # No shlex round-trip needed: the script is uploaded as a file via SFTP
        # (not passed as a CLI argument), so shell quoting is already correct.
        return cmd_str

    # ------------------------------------------------------------------
    # Environment variable assembly
    # ------------------------------------------------------------------

    @staticmethod
    def _build_env(
        code_package_metadata: str,
        code_package_sha: str,
        code_package_url: str,
        datastore_type: str,
        sandbox_id: str,
        sandbox_env_override: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Assemble the full env dict exported inline in the step script."""
        proxy_service_metadata = DEFAULT_METADATA == "service"
        env: dict[str, str] = {
            "METAFLOW_CODE_METADATA": code_package_metadata,
            "METAFLOW_CODE_SHA": code_package_sha,
            "METAFLOW_CODE_URL": code_package_url,
            "METAFLOW_CODE_DS": datastore_type,
            "METAFLOW_USER": util.get_username(),
            "METAFLOW_DEFAULT_DATASTORE": datastore_type,
            "METAFLOW_DEFAULT_METADATA": (
                "local" if proxy_service_metadata else DEFAULT_METADATA
            ),
            "METAFLOW_AKASH_WORKLOAD": "1",
            "METAFLOW_AKASH_DSEQ": sandbox_id,
        }

        if SERVICE_INTERNAL_URL:
            env["METAFLOW_SERVICE_URL"] = SERVICE_INTERNAL_URL

        from metaflow.metaflow_config_funcs import config_values

        for k, v in config_values():
            if (
                k.startswith("METAFLOW_DATASTORE_SYSROOT_")
                or k.startswith("METAFLOW_DATATOOLS_")
                or k.startswith("METAFLOW_S3")
                or k.startswith("METAFLOW_CARD_S3")
                or k.startswith("METAFLOW_CONDA")
                or k.startswith("METAFLOW_SERVICE")
            ):
                env[k] = v

        for var in _FORWARDED_CREDENTIAL_VARS:
            val = os.environ.get(var)
            if val:
                env[var] = val

        if sandbox_env_override:
            env.update(sandbox_env_override)

        return env

    # ------------------------------------------------------------------
    # Launch + wait
    # ------------------------------------------------------------------

    def launch(
        self,
        step_name: str,
        step_cli: str,
        task_spec: dict[str, str],
        code_package_metadata: str,
        code_package_sha: str,
        code_package_url: str,
        datastore_type: str,
        image: str | None = None,
        cpu: int = 1,
        memory: int = 1024,
        gpu: str | None = None,
        timeout: int = 600,
        env: dict[str, str] | None = None,
        on_log: Callable[[str, str], None] | None = None,
    ) -> None:
        """Deploy on Akash and execute the step command."""
        from sandrun.backend import Resources
        from sandrun.backend import SandboxConfig

        self._backend = AkashBackend()

        # Minimal config passed to the backend — env is injected inline in the script.
        config = SandboxConfig(
            image=image or "python:3.11-slim",
            env={},  # Metaflow vars are exported inline in the script (see _command)
            resources=Resources(cpu=cpu, memory_mb=memory, gpu=gpu),
            timeout=timeout,
        )
        self._sandbox_id = self._backend.create(config)

        sandbox_env = self._build_env(
            code_package_metadata,
            code_package_sha,
            code_package_url,
            datastore_type,
            self._sandbox_id,
            sandbox_env_override=env,
        )

        cmd_str = self._command(
            code_package_metadata,
            code_package_url,
            step_name,
            [step_cli],
            task_spec,
            datastore_type,
            sandbox_env,
        )

        # Deliver code package via stager (no S3 required).
        if self._stager is not None:
            self._stager.deliver(self._backend, self._sandbox_id)

        # Stage dependency packages offline.
        if self._installer is not None:
            self._installer.stage(self._backend, self._sandbox_id)

        run_cmd = cmd_str

        _, debug_dump = _debug_settings()
        if debug_dump:
            import time

            os.makedirs(debug_dump, exist_ok=True)
            ts = int(time.time() * 1000)
            dump_path = os.path.join(debug_dump, f"akash-{step_name}-{ts}.sh")
            with open(dump_path, "w", encoding="utf-8") as f:
                f.write(run_cmd)

        if on_log is not None:
            def _on_stdout(line: str) -> None:
                on_log(line, "stdout")

            def _on_stderr(line: str) -> None:
                on_log(line, "stderr")
        else:
            _on_stdout = _on_stderr = None

        self._result = self._backend.exec_script_streaming(
            self._sandbox_id,
            run_cmd,
            timeout=timeout,
            on_stdout=_on_stdout,
            on_stderr=_on_stderr,
        )
        self._log_streamed = on_log is not None

    def cleanup(self) -> None:
        """Destroy the Akash deployment. Best-effort, never raises."""
        import contextlib

        keep_debug, _ = _debug_settings()
        if keep_debug:
            return
        if self._sandbox_id and self._backend:
            with contextlib.suppress(Exception):
                self._backend.destroy(self._sandbox_id)
            self._sandbox_id = None

    def wait(self, echo: Any) -> None:
        """Stream buffered output, clean up, and propagate exit code.

        Does NOT raise on non-zero exit — the Metaflow runtime checks the
        subprocess exit code directly and handles retries itself.
        """
        if self._result is None:
            raise AkashException("No result — was launch() called?")

        if not self._log_streamed:
            result: ExecResult = self._result
            if result.stdout:
                for line in result.stdout.splitlines():
                    echo(line, stream="stderr")
            if result.stderr:
                for line in result.stderr.splitlines():
                    echo(line, stream="stderr")

        exit_code = self._result.exit_code
        sandbox_id = self._sandbox_id

        self.cleanup()

        if exit_code != 0:
            echo(
                f"Akash task finished with exit code {exit_code}. "
                f"dseq={sandbox_id or '<unknown>'}",
                stream="stderr",
            )
            sys.exit(exit_code)
