"""Click CLI for Akash execution: ``python flow.py akash step ...``

Layer: CLI (same level as Metaflow Integration)
May only import from: .akash_executor, metaflow stdlib

Follows the exact same pattern as ``metaflow.plugins.aws.batch.batch_cli``.
Metaflow discovers this via the ``CLIS_DESC`` entry in ``plugins/__init__.py``.
"""

from __future__ import annotations

import glob
import json
import os
import sys
import traceback

from metaflow import util
from metaflow._vendor import click
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY
from metaflow.metadata_provider.util import sync_local_metadata_from_datastore
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.unbounded_foreach import UBF_CONTROL
from metaflow.unbounded_foreach import UBF_TASK

from .akash_executor import AkashExecutor


def _replay_task_metadata_to_service(ctx, run_id, step_name, task_id):
    if ctx.obj.metadata.TYPE != "service":
        return

    from metaflow.plugins.metadata_providers.local import LocalMetadataProvider

    meta_dir = LocalMetadataProvider._get_metadir(
        ctx.obj.flow.name, run_id, step_name, task_id
    )
    if not meta_dir:
        return

    metadata_payload = []
    artifact_payload = []
    for path in glob.glob(os.path.join(meta_dir, "sysmeta_*.json")):
        with open(path) as f:
            metadata_payload.append(json.load(f))
    for path in glob.glob(os.path.join(meta_dir, "*_artifact_*.json")):
        with open(path) as f:
            artifact_payload.append(json.load(f))

    if not metadata_payload and not artifact_payload:
        return

    provider_cls = ctx.obj.metadata.__class__
    base_url = provider_cls._obj_path(ctx.obj.flow.name, run_id, step_name, task_id)

    ctx.obj.metadata.register_task_id(run_id, step_name, task_id)
    if metadata_payload:
        provider_cls._request(
            ctx.obj.monitor,
            base_url + "/metadata",
            "POST",
            metadata_payload,
        )
    if artifact_payload:
        provider_cls._request(
            ctx.obj.monitor,
            base_url + "/artifact",
            "POST",
            artifact_payload,
        )


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Akash Network execution.")
def akash():
    pass


@akash.command(
    help="Execute a single task on Akash Network. "
    "This command calls the top-level step command inside an Akash deployment "
    "with the given options. Typically you do not call this command "
    "directly; it is used internally by Metaflow."
)
@click.argument("step-name")
@click.argument("code-package-metadata")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--executable", help="Python executable override.")
@click.option("--image", help="Container image for the Akash deployment.")
@click.option("--cpu", default=1, type=int, help="Number of CPU cores.")
@click.option("--memory", default=1024, type=int, help="Memory in MB.")
@click.option("--gpu", default=None, help="Number of GPUs.")
@click.option("--timeout", default=600, type=int, help="Timeout in seconds.")
@click.option(
    "--env-var",
    "env_vars",
    multiple=True,
    default=None,
    help="User env vars from @akash(env={}). Format: KEY=VALUE, multiple allowed.",
)
@click.option("--run-id", help="Passed to the top-level 'step'.")
@click.option("--task-id", help="Passed to the top-level 'step'.")
@click.option("--input-paths", help="Passed to the top-level 'step'.")
@click.option("--split-index", help="Passed to the top-level 'step'.")
@click.option("--clone-path", help="Passed to the top-level 'step'.")
@click.option("--clone-run-id", help="Passed to the top-level 'step'.")
@click.option("--tag", multiple=True, default=None, help="Passed to the top-level 'step'.")
@click.option("--namespace", default=None, help="Passed to the top-level 'step'.")
@click.option("--retry-count", default=0, help="Passed to the top-level 'step'.")
@click.option("--max-user-code-retries", default=0, help="Passed to the top-level 'step'.")
@click.option(
    "--ubf-context",
    default=None,
    type=click.Choice(["none", UBF_CONTROL, UBF_TASK]),
)
@click.option(
    "--code-package-local-path",
    default=None,
    help="Local path to the code package tarball for backend-native delivery "
    "(TarballStager). When provided, the package is uploaded via backend.upload() "
    "instead of downloaded from the remote datastore URL.",
)
@click.option(
    "--deps-staging-dir",
    default=None,
    help="Local path to a pre-prepared dependency staging directory "
    "(CondaOfflineInstaller). When provided, packages are uploaded via "
    "backend.upload() and installed offline (no-egress safe).",
)
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_metadata,
    code_package_sha,
    code_package_url,
    executable=None,
    image=None,
    cpu=1,
    memory=1024,
    gpu=None,
    timeout=600,
    env_vars=None,
    code_package_local_path=None,
    deps_staging_dir=None,
    **kwargs,
):
    def echo(msg, stream="stderr", **kw):
        msg = util.to_unicode(msg)
        ctx.obj.echo_always(msg, err=(stream == "stderr"), **kw)

    executable = ctx.obj.environment.executable(step_name, executable)
    entrypoint = f"{executable} -u {os.path.basename(sys.argv[0])}"

    top_params = dict(ctx.parent.parent.params)
    if ctx.obj.metadata.TYPE == "service":
        top_params["metadata"] = "local"
    top_args = " ".join(util.dict_to_cli_options(top_params))

    # Handle long input_paths by splitting into env vars
    input_paths = kwargs.get("input_paths")
    split_vars = None
    if input_paths:
        max_size = 30 * 1024
        split_vars = {
            f"METAFLOW_INPUT_PATHS_{i // max_size}": input_paths[i : i + max_size]
            for i in range(0, len(input_paths), max_size)
        }
        kwargs["input_paths"] = "".join(f"${{{s}}}" for s in split_vars)

    step_args = " ".join(util.dict_to_cli_options(kwargs))
    step_cli = f"{entrypoint} {top_args} step {step_name} {step_args}"

    node = ctx.obj.graph[step_name]
    retry_count = kwargs.get("retry_count", 0)

    task_spec = {
        "flow_name": ctx.obj.flow.name,
        "step_name": step_name,
        "run_id": kwargs["run_id"],
        "task_id": kwargs["task_id"],
        "retry_count": str(retry_count),
    }

    env = {"METAFLOW_FLOW_FILENAME": os.path.basename(sys.argv[0])}
    env_deco = [deco for deco in node.decorators if deco.name == "environment"]
    if env_deco:
        env.update(env_deco[0].attributes["vars"])
    if split_vars:
        env.update(split_vars)
    if env_vars:
        for item in list(env_vars):
            key, _, value = item.partition("=")
            if key:
                env[key] = value

    # Build stager and installer from CLI args
    stager = None
    if code_package_local_path and os.path.isfile(code_package_local_path):
        from sandrun.stager import TarballStager

        stager = TarballStager(code_package_local_path)

    installer = None
    if deps_staging_dir and os.path.isdir(deps_staging_dir):
        from sandrun.installer import CondaOfflineInstaller

        try:
            installer = CondaOfflineInstaller.from_staged(deps_staging_dir)
        except Exception as e:
            echo(
                f"[akash] Failed to load dep installer from {deps_staging_dir!r}: {e}. "
                "Falling back to bootstrap_commands()."
            )

    def _sync_metadata():
        if ctx.obj.metadata.TYPE in ("local", "service"):
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                ctx.obj.flow_datastore.get_task_datastore(
                    kwargs["run_id"], step_name, kwargs["task_id"]
                ),
            )
            if ctx.obj.metadata.TYPE == "service":
                try:
                    _replay_task_metadata_to_service(
                        ctx, kwargs["run_id"], step_name, kwargs["task_id"]
                    )
                except Exception as e:
                    echo(f"Akash metadata replay to service failed: {util.to_unicode(e)}")

    def _on_log(line: str, _stream: str) -> None:
        echo(line, stream="stderr")

    executor = AkashExecutor(
        ctx.obj.environment,
        stager=stager,
        installer=installer,
    )
    try:
        executor.launch(
            step_name,
            step_cli,
            task_spec,
            code_package_metadata,
            code_package_sha,
            code_package_url,
            ctx.obj.flow_datastore.TYPE,
            image=image,
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            timeout=timeout,
            env=env,
            on_log=_on_log,
        )
    except Exception:
        traceback.print_exc()
        executor.cleanup()
        _sync_metadata()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    try:
        executor.wait(echo=echo)
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        executor.cleanup()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    finally:
        _sync_metadata()
