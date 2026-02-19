from datetime import datetime, timezone
import json
import logging
import ssl
import websocket
from config import settings

logger = logging.getLogger(__name__)


class ZfsError(Exception):
    pass


class TrueNASClient:
    def __init__(self):
        self.url = None
        self.ws = None
        self.authed = False
        self._id = 0

    def _next_id(self):
        self._id += 1
        return self._id

    def _send_json(self, payload):
        self.ws.send(json.dumps(payload))

    def _recv_json(self):
        return json.loads(self.ws.recv())

    def _recv_until(self, *, expected_msg=None, expected_id=None):
        while True:
            msg = self._recv_json()

            if msg.get("msg") == "ping":
                self._send_json({"msg": "pong"})
                continue

            if expected_msg and msg.get("msg") != expected_msg:
                continue

            if expected_id is not None and msg.get("id") != expected_id:
                continue

            return msg

    def connect(self):
        if self.ws:
            return

        from config import is_configured

        if not is_configured():
            raise ZfsError("TrueNAS middleware not configured")

        self.url = settings.TRUENAS_WS_URL

        sslopt = None
        if not settings.TRUENAS_VERIFY_TLS:
            sslopt = {"cert_reqs": ssl.CERT_NONE}

        logger.info("WS connect start url=%s", self.url)

        try:
            self.ws = websocket.create_connection(
                self.url,
                timeout=10,
                sslopt=sslopt,
            )

            self._send_json({
                "msg": "connect",
                "version": "1",
                "support": ["1"],
            })

            handshake = self._recv_until(expected_msg="connected")
            if handshake.get("msg") != "connected":
                raise ZfsError(f"Middleware handshake failed: {handshake}")

            self._authenticate()
            logger.info("WS connect+auth success")
        except Exception:
            self.close()
            raise

    def _authenticate(self):
        if self.authed:
            return

        api_key = settings.TRUENAS_API_KEY
        if not api_key:
            raise ZfsError("TRUENAS_API_KEY is not configured")

        login_id = self._next_id()
        frame = {
            "id": login_id,
            "msg": "method",
            "method": "auth.login_with_api_key",
            "params": [api_key],
        }
        self._send_json(frame)

        resp = self._recv_until(expected_msg="result", expected_id=login_id)
        if resp.get("error"):
            raise ZfsError(f"TrueNAS auth error: {resp}")
        if resp.get("result") is not True:
            raise ZfsError(f"TrueNAS auth rejected: {resp}")

        self.authed = True

    def call(self, method, *params):
        if self.ws is None:
            raise ZfsError("Client not connected")
        if not self.authed:
            raise ZfsError("Client not authenticated")

        req_id = self._next_id()
        self._send_json({
            "id": req_id,
            "msg": "method",
            "method": method,
            "params": list(params),
        })

        resp = self._recv_until(expected_msg="result", expected_id=req_id)
        if resp.get("error"):
            raise ZfsError(resp["error"])
        return resp.get("result")

    def subscribe(self, collection, sub_id):
        if self.ws is None:
            raise ZfsError("Client not connected")
        if not self.authed:
            raise ZfsError("Client not authenticated")
        self._send_json({
            "id": self._next_id(),
            "msg": "sub",
            "name": collection,
            "id": sub_id,
        })

    def unsubscribe(self, sub_id):
        if self.ws is None:
            raise ZfsError("Client not connected")
        if not self.authed:
            raise ZfsError("Client not authenticated")
        self._send_json({
            "id": self._next_id(),
            "msg": "unsub",
            "id": sub_id,
        })

    def recv(self):
        if self.ws is None:
            raise ZfsError("Client not connected")
        return json.loads(self.ws.recv())

    def close(self):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
            finally:
                logger.info("WS closed")
        self.ws = None
        self.authed = False


# Backward-compatible alias for existing imports.
TrueNASMiddlewareClient = TrueNASClient


def _ensure_client(client=None):
    if client is not None:
        return client, False
    created = TrueNASClient()
    created.connect()
    return created, True


def list_snapshots(dataset=None, client=None):
    client, should_close = _ensure_client(client)
    try:
        filters = []
        if dataset:
            filters = [["dataset", "=", dataset]]

        snaps = client.call("zfs.snapshot.query", filters) or []

        for s in snaps:
            raw = s.get("properties", {}).get("creation", {}).get("parsed")
            if raw:
                try:
                    s["created_at"] = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except Exception:
                    s["created_at"] = None
            else:
                s["created_at"] = None

            s["snapshot_name"] = s.get("name", "").split("@")[-1]
            s["full_name"] = s.get("name")

        return snaps
    finally:
        if should_close:
            client.close()


def list_datasets_with_snapshots(client=None):
    client, should_close = _ensure_client(client)
    try:
        datasets = client.call("zfs.dataset.query") or []
        snapshots = client.call("zfs.snapshot.query") or []

        pools = {}
        for d in datasets:
            name = d.get("name")
            if not name:
                continue
            pool = name.split("/")[0]
            pools.setdefault(pool, {})
            pools[pool].setdefault(name, [])

        for s in snapshots:
            ds = s.get("dataset")
            if not ds:
                continue
            pool = ds.split("/")[0]
            if pool in pools and ds in pools[pool]:
                pools[pool][ds].append(s)

        return pools
    finally:
        if should_close:
            client.close()


def rollback_snapshot(dataset, snapshot, client=None):
    client, should_close = _ensure_client(client)
    try:
        return client.call("zfs.snapshot.rollback", f"{dataset}@{snapshot}")
    finally:
        if should_close:
            client.close()


def clone_snapshot(dataset, snapshot, target, client=None):
    client, should_close = _ensure_client(client)
    try:
        return client.call("zfs.snapshot.clone", f"{dataset}@{snapshot}", target)
    finally:
        if should_close:
            client.close()


def get_job(job_id, client=None):
    client, should_close = _ensure_client(client)
    try:
        return client.call("core.get_jobs", [["id", "=", job_id]])
    finally:
        if should_close:
            client.close()


def get_pools_health(client=None):
    client, should_close = _ensure_client(client)
    try:
        pools = client.call("pool.query") or []
        return {p.get("name"): p.get("status", "UNKNOWN") for p in pools}
    finally:
        if should_close:
            client.close()


def validate_restore_paths(source_path: str, target_path: str):
    """Guard for restore operations.

    Ensures:
      - source_path contains ".zfs/snapshot/"
      - target_path does NOT contain ".zfs/snapshot"
      - both resolve under /data and do not escape via symlinks

    Raises ZfsError on invalid paths.
    """
    import os

    if ".zfs/snapshot/" not in source_path:
        raise ZfsError("Source must be inside a snapshot path")
    if ".zfs/snapshot" in target_path:
        raise ZfsError("Target must not be inside a snapshot path")

    base_root = os.path.realpath("/data")
    src_real = os.path.realpath(source_path)
    tgt_real = os.path.realpath(target_path)

    def _in_data(p):
        return p == base_root or p.startswith(base_root + os.sep)

    if not _in_data(src_real) or not _in_data(tgt_real):
        raise ZfsError("Restore paths must reside under /data")

    return True


def get_dataset_space(dataset_objects):
    results = {}

    for d in dataset_objects:
        name = d.get("name")
        if not name:
            continue

        results[name] = {
            "used": d.get("properties", {}).get("used", {}).get("parsed", 0),
            "avail": d.get("properties", {}).get("available", {}).get("parsed", 0),
        }

    return results


def snapshot_diff(dataset, a, b, client=None):
    client, should_close = _ensure_client(client)
    try:
        result = client.call(
            "zfs.snapshot.get_diff",
            f"{dataset}@{a}",
            f"{dataset}@{b}",
        )
        return result or {"added": [], "removed": [], "modified": []}
    finally:
        if should_close:
            client.close()


def list_snapshot_files(dataset, snapshot, path="", client=None):
    base = f"/mnt/{dataset}/.zfs/snapshot/{snapshot}"
    if path:
        middleware_path = f"{base}/{path}"
    else:
        middleware_path = base

    client, should_close = _ensure_client(client)
    try:
        try:
            entries = client.call("filesystem.listdir", middleware_path)
        except ZfsError as e:
            raise ZfsError(str(e))

        if not isinstance(entries, list):
            raise ZfsError(f"Unexpected filesystem.listdir result: {entries!r}")

        # Preserve middleware response while adding view-friendly fields.
        for entry in entries:
            t = (entry.get("type") or "").upper()
            entry["is_dir"] = t == "DIRECTORY"
            if entry["is_dir"]:
                entry["size"] = None

        entries.sort(key=lambda x: (not x.get("is_dir", False), str(x.get("name", "")).lower()))
        return entries
    finally:
        if should_close:
            client.close()
