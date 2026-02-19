import os
import json
import logging
import requests
from typing import Any
from config import settings


class MiddlewareError(Exception):
    pass


class MiddlewareClient:
    """Simple REST client for TrueNAS middleware core/call endpoint.

    Usage:
        mc = MiddlewareClient()
        mc.call("filesystem.copy", "/mnt/pool/...", "/mnt/pool/..", {"recursive": True})
    """

    def __init__(self, timeout: int = 30):
        # Fail fast if middleware config missing
        if not getattr(settings, "TRUENAS_URL", None) or not getattr(settings, "TRUENAS_API_KEY", None):
            raise RuntimeError("TrueNAS middleware not configured")
        # Use TRUENAS_URL exactly as provided; do not rewrite/force scheme
        self.base = settings.TRUENAS_URL.rstrip("/")
        self.key = settings.TRUENAS_API_KEY
        self.timeout = timeout
        # Respect the provided TRUENAS_VERIFY_TLS value. Accept string or bool.
        v = getattr(settings, "TRUENAS_VERIFY_TLS", False)
        if isinstance(v, str):
            self.verify = not (v.lower() == "false")
        else:
            self.verify = bool(v)
        self._logger = logging.getLogger("shadowportal.middleware")

    def _headers(self) -> dict:
        # Only include Authorization header as requested
        return {"Authorization": f"Bearer {self.key}"}

    def call(self, method: str, *params: Any) -> Any:
        url = f"{self.base}/api/v2.0/core/call"
        body = {"method": method, "params": list(params)}

        # Debug log the REST call (try to use app.logger if available)
        try:
            # prefer Flask app logger if present
            from app import app as _app
            _app.logger.info("REST CALL -> %s", url)
        except Exception:
            # fallback to module logger
            try:
                self._logger.info("REST CALL -> %s", url)
            except Exception:
                pass

        try:
            r = requests.post(url, headers=self._headers(), json=body, timeout=self.timeout, verify=self.verify)
        except Exception as e:
            raise MiddlewareError(f"middleware request failed: {e}")

        if r.status_code < 200 or r.status_code >= 300:
            raise MiddlewareError(f"middleware returned {r.status_code}: {r.text}")

        try:
            return r.json()
        except Exception:
            return r.text
def websocket_retry():
