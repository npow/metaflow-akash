# metaflow-akash

[![CI](https://github.com/npow/metaflow-akash/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-akash/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-akash)](https://pypi.org/project/metaflow-akash/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

Run your Metaflow steps on decentralized cloud — no AWS, GCP, or Azure required.

## The problem

ML teams running Metaflow pipelines are locked into expensive managed cloud providers for GPU and compute-heavy steps. Spot instance markets are volatile, and switching providers means rewriting infrastructure code. Akash Network offers significantly cheaper decentralized compute, but integrating it into an existing Metaflow pipeline normally requires custom tooling.

## Quick start

```bash
pip install metaflow-akash[akash]
export AKASH_KEY_NAME=mykey  # funded Akash wallet key
export METAFLOW_DEFAULT_DATASTORE=s3
```

```python
from metaflow import FlowSpec, step
from metaflow_extensions.akash.plugins.akash_decorator import AkashDecorator as akash

class TrainFlow(FlowSpec):

    @akash(cpu=4, memory=8192, image="python:3.11-slim")
    @step
    def train(self):
        import torch
        # this step runs on Akash Network
        self.result = "done"
        self.next(self.end)

    @step
    def start(self):
        self.next(self.train)

    @step
    def end(self):
        print(self.result)

if __name__ == "__main__":
    TrainFlow()
```

```bash
python train_flow.py run
```

## Install

```bash
# Core install
pip install metaflow-akash

# With Akash backend (paramiko + pyyaml for SSH/SDL)
pip install metaflow-akash[akash]
```

**Requirements:**
- [`akash` CLI](https://akash.network/docs/deployments/akash-cli/installation/) in your PATH
- A funded Akash wallet: `akash keys add mykey && export AKASH_KEY_NAME=mykey`
- A remote Metaflow datastore (S3, Azure, GCS) — `METAFLOW_DEFAULT_DATASTORE=s3`

## Usage

### Basic step on Akash

```python
@akash(cpu=2, memory=4096)
@step
def process(self):
    # runs on Akash Network, artifacts go to your configured datastore
    self.data = expensive_computation()
    self.next(self.next_step)
```

### GPU step

```python
@akash(cpu=4, memory=16384, gpu="1", image="pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime")
@step
def train(self):
    import torch
    assert torch.cuda.is_available()
    self.next(self.end)
```

### Custom image and timeout

```python
@akash(
    image="ubuntu:22.04",
    cpu=8,
    memory=32768,
    timeout=3600,  # 1 hour
)
@step
def heavy_job(self):
    ...
```

## How it works

1. `@akash` on a step redirects execution to the `akash step` CLI command.
2. The Akash executor generates an ephemeral SSH keypair and submits an SDL deployment to the Akash Network.
3. It waits for provider bids, accepts the cheapest one, sends the manifest, and waits for SSH to become reachable.
4. Your step command is uploaded as a script and executed via SSH. Metaflow artifacts are persisted to your configured datastore (S3, Azure, GCS).
5. On completion the Akash lease is closed.

Environment variables (credentials, datastore config) are injected inline in the step script — not in the SDL manifest — to handle complex values like S3 URLs and JSON blobs safely.

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
| `METAFLOW_AKASH_DEBUG` | — | Set to `1` to keep deployments and dump scripts to `/tmp/metaflow-akash-debug/` |

## Development

```bash
git clone https://github.com/npow/metaflow-akash
cd metaflow-akash
pip install -e ".[dev]"

# Unit tests (no credentials needed)
pytest tests/unit/ -v

# Lint
ruff check src/

# Type check
mypy src/
```

Integration tests require a funded Akash key and the `akash` CLI:

```bash
AKASH_KEY_NAME=mykey pytest tests/ -m integration
```

## License

[Apache 2.0](LICENSE)
