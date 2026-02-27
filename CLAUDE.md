# metaflow-akash

Run Metaflow steps on Akash Network decentralized cloud compute via the `@akash` decorator.

## Architecture

Layers (dependencies flow downward only):

```
Decorators   — src/.../plugins/akash_decorator.py
    ↓
CLI + Exec   — src/.../plugins/akash_cli.py, akash_executor.py
    ↓
Backend      — src/.../plugins/backend.py (re-export)
    ↓
sandrun      — sandrun.backends.akash.AkashBackend
    ↓
ABC          — sandrun.backend.SandboxBackend
```

**Rule: no upward imports.** The backend must never import from the decorator layer.

## Key files

| What | Where |
|------|-------|
| Plugin registration | `src/metaflow_extensions/akash/plugins/__init__.py` |
| @akash decorator | `src/metaflow_extensions/akash/plugins/akash_decorator.py` |
| CLI command handler | `src/metaflow_extensions/akash/plugins/akash_cli.py` |
| Akash executor | `src/metaflow_extensions/akash/plugins/akash_executor.py` |
| Backend re-export | `src/metaflow_extensions/akash/plugins/backend.py` |
| AkashBackend (impl) | `sandrun/src/sandrun/backends/akash.py` |

## How it works

1. `@akash` on a step causes `runtime_step_cli` to redirect execution to `akash step`.
2. `akash step` (CLI) builds an `AkashExecutor` and calls `executor.launch()`.
3. `AkashExecutor.launch()` creates an `AkashBackend` instance, calls `backend.create()` which:
   - Generates an ephemeral SSH keypair
   - Builds an Akash SDL manifest (YAML)
   - Submits a deployment transaction via `akash` CLI
   - Waits for provider bids, accepts the cheapest
   - Sends the manifest to the provider
   - Waits for SSH to become reachable
4. The step command is uploaded as a script and executed via SSH.
5. On completion, `backend.destroy()` closes the Akash lease.

## Env vars assembled in the step script

Metaflow runtime vars (METAFLOW_CODE_URL, credentials, etc.) are exported
**inline in the step bash script** rather than in the SDL env section.
This avoids YAML encoding issues for complex values (S3 URLs, JSON, etc.).

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `AKASH_KEY_NAME` | — (required) | Akash signing key name |
| `AKASH_KEYRING_BACKEND` | `os` | Keyring backend |
| `AKASH_NODE` | `https://rpc.akashnet.net:443` | RPC node |
| `AKASH_CHAIN_ID` | `akashnet-2` | Chain ID |
| `AKASH_GAS` | `auto` | Gas setting |
| `AKASH_GAS_ADJUSTMENT` | `1.5` | Gas adjustment |
| `AKASH_GAS_PRICES` | `0.025uakt` | Gas prices |
| `METAFLOW_AKASH_SDL_PRICE` | `10000` | Max bid price (uakt/block) |
| `METAFLOW_AKASH_BID_POLL_INTERVAL` | `5` | Seconds between bid polls |
| `METAFLOW_AKASH_READY_POLL_INTERVAL` | `5` | Seconds between ready polls |
| `METAFLOW_AKASH_TARGET_PLATFORM` | `linux-64` | Conda target arch |
| `METAFLOW_AKASH_DEBUG` | — | Set to `1` to keep deployments + dump scripts |

## Requirements

- `akash` CLI in PATH — https://akash.network/docs/deployments/akash-cli/installation/
- `AKASH_KEY_NAME` set to a funded Akash wallet key
- `pip install metaflow-akash[akash]` (includes paramiko + pyyaml)
- A remote Metaflow datastore (S3, Azure, GCS) — local datastore is not supported

## Usage

```python
from metaflow import FlowSpec, step

class MyFlow(FlowSpec):

    @akash(cpu=4, memory=8192)
    @step
    def train(self):
        import torch
        # runs on Akash Network
        ...

    @step
    def start(self):
        self.next(self.train)

    @step
    def end(self):
        pass
```

## Commands

```bash
# Lint
ruff check src/

# Type check
mypy src/

# Unit tests (no credentials needed)
pytest tests/unit/

# Integration tests (need AKASH_KEY_NAME + akash CLI)
AKASH_KEY_NAME=mykey pytest tests/ -m integration
```

## Conventions

- `AkashBackend` is implemented in `sandrun/src/sandrun/backends/akash.py`, not here.
  This package only provides the Metaflow integration layer.
- Env vars for Metaflow runtime are injected **inline in the step script** (not in SDL).
- The executor passes an empty `SandboxConfig.env` to avoid SDL manifest bloat.
- Backend layer docstrings must state their Layer and allowed imports.
