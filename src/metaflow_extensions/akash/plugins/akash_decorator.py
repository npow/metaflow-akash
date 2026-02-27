"""Metaflow @akash step decorator.

Layer: Metaflow Integration
May only import from: sandrun, metaflow stdlib

Provides the ``@akash`` decorator that executes Metaflow steps on
Akash Network decentralized compute. Follows the same lifecycle-hook
pattern as ``@batch`` and ``@kubernetes``.

Usage:
    @akash(cpu=4, memory=8192, image="python:3.11-slim")
    @step
    def my_step(self):
        ...
"""

from __future__ import annotations

import os
import platform
import sys
from typing import Any
from typing import ClassVar

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

# Env vars forwarded from the orchestrating machine into the Akash container.
_AKASH_AUTH_ENV_VARS = (
    "AKASH_KEY_NAME",
    "AKASH_KEYRING_BACKEND",
    "AKASH_NODE",
    "AKASH_CHAIN_ID",
    "AKASH_GAS",
    "AKASH_GAS_ADJUSTMENT",
    "AKASH_GAS_PRICES",
    "METAFLOW_AKASH_SDL_PRICE",
    "METAFLOW_AKASH_BID_POLL_INTERVAL",
    "METAFLOW_AKASH_READY_POLL_INTERVAL",
)

# Capture auth values at import time so they survive env mutations later.
_INITIAL_AKASH_ENV = {
    k: v for k, v in ((var, os.environ.get(var)) for var in _AKASH_AUTH_ENV_VARS) if v
}

# Required for nflx-extensions conda to treat @akash as a "remote command"
# and resolve linux-64 ResolvedEnvironments instead of native macOS ones.
_AKASH_REMOTE_COMMAND_ALIASES = ("akash",)


def _default_target_platform() -> str:
    machine = platform.machine().lower()
    if machine in ("aarch64", "arm64"):
        return "linux-aarch64"
    return "linux-64"


def _ensure_conda_remote_command_aliases() -> None:
    """Patch nflx-extensions CondaEnvironment so @akash gets linux-64 envs."""
    module_names = (
        "metaflow_extensions.netflix_ext.plugins.conda.conda_environment",
        "metaflow_extensions.netflix_ext.plugins.conda.conda_step_decorator",
    )
    for module_name in module_names:
        try:
            from importlib import import_module

            module = import_module(module_name)
        except Exception:
            continue
        current = tuple(getattr(module, "CONDA_REMOTE_COMMANDS", ()))
        merged = tuple(dict.fromkeys([*current, *_AKASH_REMOTE_COMMAND_ALIASES]))
        module.CONDA_REMOTE_COMMANDS = merged


class AkashException(MetaflowException):
    headline = "Akash error"


class AkashDecorator(StepDecorator):
    """Run a Metaflow step on Akash Network decentralized compute.

    Parameters
    ----------
    cpu : int
        Number of CPU cores to request (default: 1).
    memory : int
        Memory in MB to request (default: 1024).
    gpu : str | None
        Number of GPUs to request, e.g. ``"1"`` (default: None).
    image : str | None
        Container image, e.g. ``"python:3.11-slim"`` (default: ``python:3.11-slim``).
    timeout : int
        Step timeout in seconds (default: 600).
    executable : str | None
        Python executable override (default: None).
    env : dict
        Extra environment variables to set in the sandbox (default: {}).
    """

    name = "akash"

    defaults: ClassVar[dict[str, Any]] = {
        "cpu": 1,
        "memory": 1024,
        "gpu": None,
        "image": None,
        "timeout": 600,
        "executable": None,
        "env": {},
    }

    supports_conda_environment = True
    target_platform = os.environ.get(
        "METAFLOW_AKASH_TARGET_PLATFORM", _default_target_platform()
    )

    # Class-level code-package state shared across all decorator instances
    # (one upload per flow run, regardless of how many @akash steps there are).
    package_metadata: ClassVar[str | None] = None
    package_url: ClassVar[str | None] = None
    package_sha: ClassVar[str | None] = None
    package_local_path: ClassVar[str | None] = None

    # Per-step dep staging dirs (step_name -> local staging path).
    _prepared_deps: ClassVar[dict[str, str]] = {}

    def step_init(
        self,
        flow: Any,
        graph: Any,
        step_name: str,
        decorators: Any,
        environment: Any,
        flow_datastore: Any,
        logger: Any,
    ) -> None:
        _ensure_conda_remote_command_aliases()

        self._step_name = step_name
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.logger = logger
        self.attributes.setdefault("env", {})

        # Propagate akash auth vars into the step's env dict so they survive
        # into the `akash step` CLI subprocess.
        for var in _AKASH_AUTH_ENV_VARS:
            val = os.environ.get(var) or _INITIAL_AKASH_ENV.get(var)
            if val:
                self.attributes["env"].setdefault(var, val)

        # Validate: Akash requires a remote datastore for artifact persistence.
        if flow_datastore.TYPE == "local":
            raise AkashException(
                "@akash requires a remote datastore (s3, azure, gs). "
                "Configure with: METAFLOW_DEFAULT_DATASTORE=s3\n"
                "See https://docs.metaflow.org/scaling/remote-tasks/introduction"
            )

    def runtime_init(self, flow: Any, graph: Any, package: Any, run_id: str) -> None:
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self,
        task_datastore: Any,
        task_id: str,
        split_index: Any,
        input_paths: Any,
        is_cloned: bool,
        ubf_context: Any,
    ) -> None:
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)
            self._prepare_deps_once(self._step_name)

    def _prepare_deps_once(self, step_name: str) -> None:
        """Download and locally stage conda deps for *step_name* (idempotent)."""
        if step_name in AkashDecorator._prepared_deps:
            return

        specs, target_arch = _get_resolved_package_specs(
            self.environment,
            self.flow,
            self.flow_datastore.TYPE,
            step_name,
        )
        if not specs:
            return

        from sandrun.installer import CondaOfflineInstaller

        installer = CondaOfflineInstaller()
        try:
            installer.prepare(specs, target_arch)
        except Exception as e:
            self.logger(
                f"[akash] Offline dep staging failed for step '{step_name}': {e}. "
                "Falling back to bootstrap_commands().",
                head="",
                bad=False,
            )
            return

        AkashDecorator._prepared_deps[step_name] = installer._staging_dir

    def runtime_step_cli(
        self,
        cli_args: Any,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: Any,
    ) -> None:
        """Redirect this step's execution through the `akash step` CLI."""
        if os.environ.get("METAFLOW_AKASH_WORKLOAD"):
            return

        if retry_count <= max_user_code_retries:
            cli_args.commands = ["akash", "step"]
            cli_args.command_args.append(self.package_metadata)
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)

            _skip_keys = {"env"}
            cli_args.command_options.update(
                {k: v for k, v in self.attributes.items() if k not in _skip_keys}
            )

            # Serialize user env + akash auth vars as --env-var KEY=VALUE
            user_env = dict(self.attributes.get("env") or {})
            for var in _AKASH_AUTH_ENV_VARS:
                val = os.environ.get(var) or _INITIAL_AKASH_ENV.get(var)
                if val:
                    user_env.setdefault(var, val)
            if user_env:
                cli_args.command_options["env-var"] = [
                    f"{k}={v}" for k, v in user_env.items()
                ]

            if AkashDecorator.package_local_path:
                cli_args.command_options[
                    "code-package-local-path"
                ] = AkashDecorator.package_local_path

            staging_dir = AkashDecorator._prepared_deps.get(self._step_name)
            if staging_dir:
                cli_args.command_options["deps-staging-dir"] = staging_dir

            cli_args.entrypoint[0] = sys.executable

    def task_pre_step(
        self,
        step_name: str,
        task_datastore: Any,
        metadata: Any,
        run_id: str,
        task_id: str,
        flow: Any,
        graph: Any,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: Any,
        inputs: Any,
    ) -> None:
        self.metadata = metadata
        self.task_datastore = task_datastore

        if os.environ.get("METAFLOW_AKASH_WORKLOAD"):
            from metaflow.metadata_provider import MetaDatum

            entries = [
                MetaDatum(
                    field=k,
                    value=v,
                    type=k,
                    tags=[f"attempt_id:{retry_count}"],
                )
                for k, v in {
                    "akash-dseq": os.environ.get("METAFLOW_AKASH_DSEQ", ""),
                    "akash-provider": os.environ.get("METAFLOW_AKASH_PROVIDER", ""),
                }.items()
            ]
            metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self,
        step_name: str,
        flow: Any,
        graph: Any,
        is_task_ok: bool,
        retry_count: int,
        max_retries: int,
    ) -> None:
        if (
            os.environ.get("METAFLOW_AKASH_WORKLOAD")
            and hasattr(self, "metadata")
            and self.metadata.TYPE == "local"
        ):
            from metaflow.metadata_provider.util import sync_local_metadata_to_datastore
            from metaflow.metaflow_config import DATASTORE_LOCAL_DIR

            sync_local_metadata_to_datastore(DATASTORE_LOCAL_DIR, self.task_datastore)

    @classmethod
    def _save_package_once(cls, flow_datastore: Any, package: Any) -> None:
        """Upload code package once per flow run (shared across all @akash steps)."""
        if AkashDecorator.package_url is None:
            import tempfile

            fd, local_path = tempfile.mkstemp(suffix=".tar", prefix="mf-akash-code-")
            try:
                import os as _os

                with _os.fdopen(fd, "wb") as f:
                    f.write(package.blob)
            except Exception:
                import os as _os

                try:
                    _os.unlink(local_path)
                except OSError:
                    pass
                raise
            AkashDecorator.package_local_path = local_path

            url, sha = flow_datastore.save_data([package.blob], len_hint=1)[0]
            AkashDecorator.package_url = url
            AkashDecorator.package_sha = sha
            AkashDecorator.package_metadata = package.package_metadata


def _get_resolved_package_specs(
    environment: Any,
    flow: Any,
    datastore_type: str,
    step_name: str,
) -> tuple[list[Any], str]:
    """Return (PackageSpec list, target_arch) from a nflx CondaEnvironment."""
    try:
        from metaflow_extensions.netflix_ext.plugins.conda.conda_environment import (
            CondaEnvironment,
        )
    except ImportError:
        return [], "linux-64"

    if not isinstance(environment, CondaEnvironment) or environment.conda is None:
        return [], "linux-64"

    try:
        _, arch, _, resolved_env = CondaEnvironment.extract_merged_reqs_for_step(
            environment.conda, flow, datastore_type, step_name,
        )
    except Exception:
        return [], "linux-64"

    if resolved_env is None:
        return [], arch or "linux-64"

    from sandrun._types import PackageSpec

    specs = [
        PackageSpec(
            url=spec.url,
            filename=spec.filename,
            pkg_type=spec.TYPE,
            hashes=dict(spec.pkg_hashes),
            is_real_url=getattr(spec, "is_real_url", True),
            url_format=getattr(spec, "url_format", "") or "",
            environment_marker=getattr(spec, "environment_marker", None),
        )
        for spec in resolved_env.packages
        if getattr(spec, "is_real_url", True)
    ]
    return specs, arch or "linux-64"
