"""Akash sandbox backend — re-exported from sandrun.

Layer: Concrete Backend
May only import from: sandrun.backends.akash

Install: pip install metaflow-akash[akash]
Docs:    https://akash.network/docs
"""

from sandrun.backends.akash import AkashBackend as AkashBackend

__all__ = ["AkashBackend"]
