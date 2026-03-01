"""Akash Network backend for metaflow-akash.

Layer: Concrete Backend
May only import from: sandrun.backend (ABC + dataclasses), akash CLI, paramiko

Install: pip install metaflow-akash[akash]
Docs:    https://akash.network/docs

Deploys Metaflow steps as containers on the Akash decentralized cloud.
Uses the `akash` CLI for deployment lifecycle and paramiko for SSH exec/upload.

Auth: Set AKASH_KEY_NAME in your environment (required).
      Other AKASH_* env vars are forwarded automatically to the subprocess.
"""

from __future__ import annotations

import base64
import json
import os
import shlex
import subprocess
import time
import uuid
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from typing import Callable

from sandrun.backend import ExecResult
from sandrun.backend import SandboxBackend
from sandrun.backend import SandboxConfig

# ---------------------------------------------------------------------------
# Constants / tunables
# ---------------------------------------------------------------------------

_INSTALL_HINT_PARAMIKO = (
    "paramiko is required for Akash SSH execution. Install it with:\n"
    "\n"
    "    pip install metaflow-akash[akash]\n"
    "\n"
    "See https://www.paramiko.org for details."
)

_INSTALL_HINT_AKASH_CLI = (
    "The `akash` CLI is required for Akash Network deployments. Install it from:\n"
    "\n"
    "    https://akash.network/docs/deployments/akash-cli/installation/\n"
    "\n"
    "Then configure a key:\n"
    "    akash keys add mykey\n"
    "    export AKASH_KEY_NAME=mykey\n"
)

# Seconds to wait for bids before giving up.
_DEFAULT_BID_TIMEOUT = 120
# Seconds to wait for service TCP to be reachable.
_DEFAULT_READY_TIMEOUT = 300
# Default SDL pricing per block (uakt).
_DEFAULT_SDL_PRICE = 10000


# ---------------------------------------------------------------------------
# Internal state types
# ---------------------------------------------------------------------------


@dataclass
class _LeaseId:
    dseq: str
    gseq: str
    oseq: str
    provider: str
    owner: str


@dataclass
class _DeploymentState:
    lease: _LeaseId
    ssh_host: str
    ssh_port: int
    ssh_key: Any  # paramiko.RSAKey
    sdl_path: str  # temp file path; removed on destroy()
    _ssh_client: Any = field(default=None, compare=False, repr=False)


# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------


def _check_akash_cli() -> None:
    import shutil

    if not shutil.which("akash"):
        raise RuntimeError(_INSTALL_HINT_AKASH_CLI)


def _check_paramiko() -> None:
    try:
        import paramiko  # noqa: F401
    except ImportError:
        raise ImportError(_INSTALL_HINT_PARAMIKO) from None


# ---------------------------------------------------------------------------
# akash CLI helpers
# ---------------------------------------------------------------------------


def _akash_env() -> dict[str, str]:
    """Build subprocess env with Akash defaults filled in."""
    env = dict(os.environ)
    env.setdefault("AKASH_KEYRING_BACKEND", "os")
    env.setdefault("AKASH_NODE", "https://rpc.akashnet.net:443")
    env.setdefault("AKASH_CHAIN_ID", "akashnet-2")
    env.setdefault("AKASH_GAS", "auto")
    env.setdefault("AKASH_GAS_ADJUSTMENT", "1.5")
    env.setdefault("AKASH_GAS_PRICES", "0.025uakt")
    return env


def _run_json(args: list[str], timeout: int = 60) -> dict:
    """Run an akash CLI command and return parsed JSON output."""
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_akash_env(),
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"akash command failed: {shlex.join(args)}\n"
            f"stdout: {result.stdout.strip()}\n"
            f"stderr: {result.stderr.strip()}"
        )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"raw": result.stdout}


def _get_key_name() -> str:
    key_name = os.environ.get("AKASH_KEY_NAME", "").strip()
    if not key_name:
        raise RuntimeError(
            "AKASH_KEY_NAME is not set.\n"
            "Set it to the name of your Akash signing key:\n"
            "    export AKASH_KEY_NAME=mykey\n"
            "List available keys: akash keys list"
        )
    return key_name


def _get_address() -> str:
    """Return the bech32 Akash address for the configured key."""
    key_name = _get_key_name()
    result = subprocess.run(
        ["akash", "keys", "show", key_name, "-a"],
        capture_output=True,
        text=True,
        env=_akash_env(),
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to get Akash address for key '{key_name}'.\n"
            f"Verify key exists: akash keys list\n"
            f"Error: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def _extract_dseq(tx: dict) -> str:
    """Parse deployment sequence number from a tx result JSON."""
    # Search logs[].events[].attributes for dseq
    for log_entry in tx.get("logs") or []:
        for event in log_entry.get("events") or []:
            if "deployment" in event.get("type", "").lower():
                for attr in event.get("attributes") or []:
                    if attr.get("key") == "dseq":
                        val = attr.get("value", "").strip('"')
                        if val.isdigit():
                            return val
    # Newer Cosmos SDK: top-level events with base64-encoded attribute keys/values
    for event in tx.get("events") or []:
        if "deployment" in event.get("type", "").lower():
            for attr in event.get("attributes") or []:
                key_raw = attr.get("key", "")
                val_raw = attr.get("value", "")
                # Decode base64 if needed
                for maybe_b64 in (key_raw, val_raw):
                    try:
                        decoded = base64.b64decode(maybe_b64 + "==").decode()
                        if decoded == "dseq":
                            val_decoded = base64.b64decode(val_raw + "==").decode().strip('"')
                            if val_decoded.isdigit():
                                return val_decoded
                    except Exception:
                        pass
                if key_raw == "dseq" and val_raw.strip('"').isdigit():
                    return val_raw.strip('"')
    raise RuntimeError(
        "Could not extract DSEQ from deployment tx.\n"
        f"tx result: {json.dumps(tx, indent=2)[:2000]}"
    )


def _create_deployment(sdl_path: str) -> str:
    """Submit a deployment creation tx and return the DSEQ."""
    key_name = _get_key_name()
    tx = _run_json(
        [
            "akash", "tx", "deployment", "create", sdl_path,
            "--from", key_name,
            "--broadcast-mode", "block",
            "-y", "-o", "json",
        ],
        timeout=120,
    )
    code = tx.get("code", 0)
    if code != 0:
        raise RuntimeError(
            f"Deployment creation tx failed (code={code}).\n"
            f"raw_log: {tx.get('raw_log', '')}"
        )
    return _extract_dseq(tx)


def _wait_for_bids(dseq: str, owner: str, timeout: int = _DEFAULT_BID_TIMEOUT) -> dict:
    """Poll for open bids and return the cheapest one."""
    poll = float(os.environ.get("METAFLOW_AKASH_BID_POLL_INTERVAL", "5"))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = _run_json(
                [
                    "akash", "query", "market", "bid", "list",
                    "--owner", owner, "--dseq", dseq,
                    "-o", "json",
                ],
                timeout=30,
            )
        except Exception:
            time.sleep(poll)
            continue
        open_bids = [
            b for b in (result.get("bids") or [])
            if b.get("bid", {}).get("state") == "open"
        ]
        if open_bids:
            def _price(b: dict) -> int:
                try:
                    return int(b["bid"].get("price", {}).get("amount", 999_999_999))
                except (ValueError, TypeError, KeyError):
                    return 999_999_999

            best = min(open_bids, key=_price)
            bid_id = best["bid"]["bid_id"]
            return {
                "dseq": str(bid_id.get("dseq", dseq)),
                "gseq": str(bid_id.get("gseq", "1")),
                "oseq": str(bid_id.get("oseq", "1")),
                "provider": bid_id.get("provider", ""),
            }
        time.sleep(poll)

    raise RuntimeError(
        f"No bids received for deployment dseq={dseq} within {timeout}s.\n"
        "Ensure your Akash wallet has enough AKT for deployment fees.\n"
        f"Check manually: akash query market bid list --owner {owner} --dseq {dseq}"
    )


def _create_lease(dseq: str, gseq: str, oseq: str, provider: str) -> None:
    """Accept the best bid by creating a lease on-chain."""
    key_name = _get_key_name()
    tx = _run_json(
        [
            "akash", "tx", "market", "lease", "create",
            "--dseq", dseq, "--gseq", gseq, "--oseq", oseq,
            "--provider", provider,
            "--from", key_name,
            "--broadcast-mode", "block",
            "-y", "-o", "json",
        ],
        timeout=120,
    )
    code = tx.get("code", 0)
    if code != 0:
        raise RuntimeError(
            f"Lease creation tx failed (code={code}).\n"
            f"raw_log: {tx.get('raw_log', '')}"
        )


def _send_manifest(sdl_path: str, dseq: str, gseq: str, oseq: str, provider: str) -> None:
    """Push the SDL manifest to the provider node."""
    key_name = _get_key_name()
    result = subprocess.run(
        [
            "akash", "provider", "send-manifest", sdl_path,
            "--dseq", dseq, "--gseq", gseq, "--oseq", oseq,
            "--provider", provider,
            "--from", key_name,
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=_akash_env(),
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"send-manifest failed.\n"
            f"stdout: {result.stdout.strip()}\n"
            f"stderr: {result.stderr.strip()}"
        )


def _get_lease_status(dseq: str, gseq: str, oseq: str, provider: str) -> dict:
    key_name = _get_key_name()
    result = subprocess.run(
        [
            "akash", "provider", "lease-status",
            "--dseq", dseq, "--gseq", gseq, "--oseq", oseq,
            "--provider", provider, "--from", key_name,
            "-o", "json",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        env=_akash_env(),
    )
    if result.returncode != 0:
        return {}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}


def _wait_for_service(
    dseq: str, gseq: str, oseq: str, provider: str,
    timeout: int = _DEFAULT_READY_TIMEOUT,
) -> tuple[str, int]:
    """Poll lease-status until SSH port is forwarded; return (host, port)."""
    poll = float(os.environ.get("METAFLOW_AKASH_READY_POLL_INTERVAL", "5"))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = _get_lease_status(dseq, gseq, oseq, provider)
        forwarded = (
            status.get("forwarded_ports")
            or status.get("forwardedPorts")
            or {}
        )
        for svc_ports in forwarded.values():
            for p in svc_ports or []:
                if p.get("port") == 22 and p.get("proto", "").upper() == "TCP":
                    host = p.get("host", "")
                    ext_port = int(p.get("externalPort") or 0)
                    if host and ext_port:
                        return host, ext_port
        time.sleep(poll)

    raise RuntimeError(
        f"Service did not expose SSH within {timeout}s (dseq={dseq}).\n"
        f"Check: akash provider lease-status "
        f"--dseq {dseq} --gseq {gseq} --oseq {oseq} --provider {provider}"
    )


def _close_deployment(dseq: str) -> None:
    key_name = _get_key_name()
    subprocess.run(
        [
            "akash", "tx", "deployment", "close",
            "--dseq", dseq,
            "--from", key_name,
            "--broadcast-mode", "sync",
            "-y",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=_akash_env(),
    )


def _wait_for_tcp(host: str, port: int, timeout: int = 120) -> None:
    """Wait until TCP port is reachable (SSH server is up)."""
    import socket

    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=5):
                return
        except OSError as e:
            last_err = e
        time.sleep(3)
    raise RuntimeError(
        f"SSH on {host}:{port} did not become reachable within {timeout}s.\n"
        f"Last error: {last_err}"
    )


# ---------------------------------------------------------------------------
# SDL builder
# ---------------------------------------------------------------------------


def _build_sdl(config: SandboxConfig, ssh_pubkey: str) -> str:
    """Return an Akash SDL 2.0 YAML string for the deployment."""
    try:
        import yaml
    except ImportError:
        raise ImportError(
            "pyyaml is required for Akash SDL generation. Install it with:\n"
            "    pip install metaflow-akash[akash]\n"
        ) from None

    image = config.image or "python:3.11-slim"
    cpu_units = max(1, config.resources.cpu)
    memory_mb = max(512, config.resources.memory_mb)
    price = int(os.environ.get("METAFLOW_AKASH_SDL_PRICE", str(_DEFAULT_SDL_PRICE)))

    # Startup script: install sshd if absent, configure authorized_keys, start sshd.
    # Base64-encoded to avoid YAML/shell quoting issues in the SDL args field.
    startup_script = (
        "#!/bin/bash\n"
        "set -e\n"
        "if ! command -v sshd >/dev/null 2>&1; then\n"
        "  (apt-get update -qq && apt-get install -y -qq openssh-server 2>/dev/null)"
        " || apk add --no-cache openssh 2>/dev/null || true\n"
        "fi\n"
        "mkdir -p /root/.ssh\n"
        "echo \"$AKASH_SSH_PUBKEY\" >> /root/.ssh/authorized_keys\n"
        "chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys\n"
        "mkdir -p /var/run/sshd /run/sshd\n"
        "sed -i 's/#\\?PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config 2>/dev/null || true\n"
        "sed -i 's/#\\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config 2>/dev/null || true\n"
        "ssh-keygen -A 2>/dev/null || true\n"
        "exec /usr/sbin/sshd -D\n"
    )
    b64_script = base64.b64encode(startup_script.encode()).decode()
    startup_cmd = f"echo {b64_script} | base64 -d | bash"

    # Env vars: only SSH pubkey goes in the SDL env section.
    # Metaflow runtime vars are injected inline in the step script instead
    # (avoids SDL size bloat and YAML quoting edge cases for complex values).
    sdl_env_list = [f"AKASH_SSH_PUBKEY={ssh_pubkey}"]

    # GPU section (optional)
    gpu_resources: dict = {}
    if config.resources.gpu:
        try:
            gpu_count = int(config.resources.gpu)
        except (ValueError, TypeError):
            gpu_count = 1
        gpu_resources = {
            "units": gpu_count,
            "attributes": {"vendor": {"nvidia": [{"model": "*"}]}},
        }

    compute_resources: dict = {
        "cpu": {"units": float(cpu_units)},
        "memory": {"size": f"{memory_mb}Mi"},
        "storage": [{"size": "10Gi"}],
    }
    if gpu_resources:
        compute_resources["gpu"] = gpu_resources

    sdl_doc = {
        "version": "2.0",
        "services": {
            "worker": {
                "image": image,
                "command": ["/bin/bash"],
                "args": ["-c", startup_cmd],
                "expose": [{"port": 22, "as": 22, "to": [{"global": True}]}],
                "env": sdl_env_list,
            }
        },
        "profiles": {
            "compute": {
                "worker": {"resources": compute_resources},
            },
            "placement": {
                "akash": {
                    "pricing": {
                        "worker": {"denom": "uakt", "amount": price},
                    },
                },
            },
        },
        "deployment": {
            "worker": {
                "akash": {"profile": "worker", "count": 1},
            },
        },
    }

    # Prepend the YAML version header (yaml.dump omits it, Akash requires it)
    return "---\n" + yaml.dump(sdl_doc, default_flow_style=False, allow_unicode=True, width=10000)


# ---------------------------------------------------------------------------
# SFTP helpers
# ---------------------------------------------------------------------------


def _sftp_makedirs(sftp: Any, remote_path: str) -> None:
    """Recursively create remote directories via SFTP."""
    if not remote_path or remote_path == "/":
        return
    try:
        sftp.stat(remote_path)
        return
    except IOError:
        pass
    parent = str(Path(remote_path).parent)
    if parent != remote_path:
        _sftp_makedirs(sftp, parent)
    try:
        sftp.mkdir(remote_path)
    except IOError:
        pass  # race: another call already created it


# ---------------------------------------------------------------------------
# Backend implementation
# ---------------------------------------------------------------------------


class AkashBackend(SandboxBackend):
    """Runs tasks on Akash Network decentralized cloud.

    Lifecycle
    ---------
    create()   — Deploys a container on Akash, waits for SSH to be ready.
    exec()     — Runs a command over SSH.
    upload()   — Copies a local file to the container via SFTP.
    download() — Copies a file from the container via SFTP.
    destroy()  — Closes the Akash lease and removes the deployment.

    Requirements
    ------------
    * ``akash`` CLI in PATH (https://akash.network/docs/deployments/akash-cli/)
    * ``AKASH_KEY_NAME`` env var set to a funded Akash wallet key
    * ``paramiko`` Python package (``pip install metaflow-akash[akash]``)

    The container image must be capable of running openssh-server
    (Debian/Ubuntu ``apt``, or Alpine ``apk``, or pre-installed SSH).
    The default ``python:3.11-slim`` image (Debian) works out of the box.
    """

    def __init__(self) -> None:
        _check_akash_cli()
        _check_paramiko()
        self._deployments: dict[str, _DeploymentState] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def create(self, config: SandboxConfig) -> str:
        """Deploy on Akash and return a sandbox_id (= DSEQ string)."""
        import paramiko
        import tempfile

        # 1. Generate an ephemeral SSH keypair.
        key = paramiko.RSAKey.generate(2048)
        pubkey = f"ssh-rsa {key.get_base64()}"

        # 2. Write SDL to a temp file.
        sdl_fd, sdl_path = tempfile.mkstemp(suffix=".yaml", prefix="akash-sdl-")
        try:
            with os.fdopen(sdl_fd, "w") as f:
                f.write(_build_sdl(config, pubkey))
        except Exception:
            try:
                os.unlink(sdl_path)
            except OSError:
                pass
            raise

        dseq: str | None = None
        try:
            owner = _get_address()
            dseq = _create_deployment(sdl_path)
            bid = _wait_for_bids(dseq, owner)
            gseq, oseq, provider = bid["gseq"], bid["oseq"], bid["provider"]
            _create_lease(dseq, gseq, oseq, provider)
            _send_manifest(sdl_path, dseq, gseq, oseq, provider)
            ssh_host, ssh_port = _wait_for_service(dseq, gseq, oseq, provider)
        except Exception:
            import contextlib

            if dseq:
                with contextlib.suppress(Exception):
                    _close_deployment(dseq)
            try:
                os.unlink(sdl_path)
            except OSError:
                pass
            raise

        sandbox_id = dseq
        self._deployments[sandbox_id] = _DeploymentState(
            lease=_LeaseId(dseq=dseq, gseq=gseq, oseq=oseq, provider=provider, owner=owner),
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            ssh_key=key,
            sdl_path=sdl_path,
        )

        # Wait for the SSH server to accept connections.
        _wait_for_tcp(ssh_host, ssh_port, timeout=120)

        return sandbox_id

    def destroy(self, sandbox_id: str) -> None:
        import contextlib

        state = self._deployments.pop(sandbox_id, None)
        if state is None:
            return
        if state._ssh_client is not None:
            with contextlib.suppress(Exception):
                state._ssh_client.close()
        with contextlib.suppress(OSError):
            os.unlink(state.sdl_path)
        with contextlib.suppress(Exception):
            _close_deployment(state.lease.dseq)

    # ------------------------------------------------------------------
    # SSH helpers
    # ------------------------------------------------------------------

    def _get_state(self, sandbox_id: str) -> _DeploymentState:
        state = self._deployments.get(sandbox_id)
        if state is None:
            raise KeyError(
                f"Akash sandbox '{sandbox_id}' not found. "
                "Sandbox handles do not survive Python process restarts."
            )
        return state

    def _get_ssh(self, sandbox_id: str) -> Any:
        """Return a live paramiko SSHClient, (re)connecting if needed.

        Retries on connection failure because sshd may still be initializing
        (running apt-get, ssh-keygen, config) after TCP becomes reachable.
        """
        import paramiko

        state = self._get_state(sandbox_id)
        transport = (
            state._ssh_client.get_transport()
            if state._ssh_client is not None
            else None
        )
        if transport is not None and transport.is_active():
            return state._ssh_client

        max_retries = 15
        retry_delay = 10
        last_err: Exception | None = None
        for attempt in range(max_retries):
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(
                    hostname=state.ssh_host,
                    port=state.ssh_port,
                    username="root",
                    pkey=state.ssh_key,
                    timeout=30,
                    banner_timeout=60,
                    auth_timeout=30,
                )
                state._ssh_client = client
                return client
            except Exception as e:
                last_err = e
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        raise RuntimeError(
            f"SSH connection to {state.ssh_host}:{state.ssh_port} failed after "
            f"{max_retries} attempts ({max_retries * retry_delay}s).\n"
            f"Last error: {last_err}"
        ) from last_err

    # ------------------------------------------------------------------
    # Command execution
    # ------------------------------------------------------------------

    def exec(
        self,
        sandbox_id: str,
        cmd: list[str],
        cwd: str = "/",
        timeout: int = 300,
    ) -> ExecResult:
        command = shlex.join(cmd)
        if cwd and cwd != "/":
            command = f"cd {shlex.quote(cwd)} && {command}"
        return self._exec_str(sandbox_id, command, timeout=timeout)

    def _exec_str(
        self,
        sandbox_id: str,
        command: str,
        timeout: int = 300,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        """Run a shell command string over SSH with optional log streaming."""
        ssh = self._get_ssh(sandbox_id)
        transport = ssh.get_transport()
        channel = transport.open_session()
        channel.exec_command(command)

        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        stdout_line_buf = ""
        stderr_line_buf = ""
        deadline = time.monotonic() + timeout

        while True:
            if time.monotonic() >= deadline:
                break

            got_data = False

            # Drain stdout
            while channel.recv_ready():
                got_data = True
                chunk = channel.recv(8192).decode("utf-8", errors="replace")
                stdout_chunks.append(chunk)
                if on_stdout:
                    stdout_line_buf += chunk
                    lines = stdout_line_buf.split("\n")
                    for line in lines[:-1]:
                        on_stdout(line)
                    stdout_line_buf = lines[-1]

            # Drain stderr
            while channel.recv_stderr_ready():
                got_data = True
                chunk = channel.recv_stderr(8192).decode("utf-8", errors="replace")
                stderr_chunks.append(chunk)
                if on_stderr:
                    stderr_line_buf += chunk
                    lines = stderr_line_buf.split("\n")
                    for line in lines[:-1]:
                        on_stderr(line)
                    stderr_line_buf = lines[-1]

            if channel.exit_status_ready():
                # Final drain
                while channel.recv_ready():
                    chunk = channel.recv(8192).decode("utf-8", errors="replace")
                    stdout_chunks.append(chunk)
                    if on_stdout:
                        stdout_line_buf += chunk
                while channel.recv_stderr_ready():
                    chunk = channel.recv_stderr(8192).decode("utf-8", errors="replace")
                    stderr_chunks.append(chunk)
                    if on_stderr:
                        stderr_line_buf += chunk
                break

            if not got_data:
                time.sleep(0.05)

        # Flush partial lines
        if on_stdout and stdout_line_buf:
            on_stdout(stdout_line_buf)
        if on_stderr and stderr_line_buf:
            on_stderr(stderr_line_buf)

        exit_code = channel.recv_exit_status()
        channel.close()

        return ExecResult(
            exit_code=exit_code,
            stdout="".join(stdout_chunks),
            stderr="".join(stderr_chunks),
        )

    def exec_script(
        self,
        sandbox_id: str,
        script: str,
        timeout: int = 600,
    ) -> ExecResult:
        return self.exec_script_streaming(sandbox_id, script, timeout)

    def exec_script_streaming(
        self,
        sandbox_id: str,
        script: str,
        timeout: int = 600,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        """Upload script to a temp file and execute it via SSH."""
        import io

        ssh = self._get_ssh(sandbox_id)
        remote_script = f"/tmp/akash-{uuid.uuid4().hex}.sh"

        sftp = ssh.open_sftp()
        try:
            sftp.putfo(io.BytesIO(script.encode("utf-8")), remote_script)
            sftp.chmod(remote_script, 0o700)
        finally:
            sftp.close()

        command = f"bash {shlex.quote(remote_script)}"
        try:
            return self._exec_str(
                sandbox_id,
                command,
                timeout=timeout,
                on_stdout=on_stdout,
                on_stderr=on_stderr,
            )
        finally:
            import contextlib

            with contextlib.suppress(Exception):
                self._exec_str(
                    sandbox_id, f"rm -f {shlex.quote(remote_script)}", timeout=15
                )

    # ------------------------------------------------------------------
    # File transfer
    # ------------------------------------------------------------------

    def upload(self, sandbox_id: str, local_path: str, remote_path: str) -> None:
        ssh = self._get_ssh(sandbox_id)
        sftp = ssh.open_sftp()
        try:
            _sftp_makedirs(sftp, str(Path(remote_path).parent))
            sftp.put(local_path, remote_path)
        finally:
            sftp.close()

    def download(self, sandbox_id: str, remote_path: str, local_path: str) -> None:
        ssh = self._get_ssh(sandbox_id)
        sftp = ssh.open_sftp()
        try:
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            sftp.get(remote_path, local_path)
        finally:
            sftp.close()
