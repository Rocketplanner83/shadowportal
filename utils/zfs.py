from datetime import datetime, timezone
import json
import ssl
import websocket
from config import settings


class ZfsError(Exception):
    pass


class TrueNASMiddlewareClient:
    def __init__(self):
        # Do not resolve WS URL at import/instance creation time to avoid
        # raising when settings are not configured. The URL will be
        # resolved in `connect()` after checking configuration.
        self.url = None
        self.ws = None
        self._id = 0

    def _next_id(self):
        self._id += 1
        return self._id

    def connect(self):
        try:
            # Ensure TrueNAS connection configuration is present
            from config import is_configured
            if not is_configured():
                raise ZfsError("TrueNAS middleware not configured")

            self.url = settings.TRUENAS_WS_URL

            sslopt = None
            if not settings.TRUENAS_VERIFY_TLS:
                sslopt = {"cert_reqs": ssl.CERT_NONE}

            self.ws = websocket.create_connection(
                self.url,
                timeout=10,
                sslopt=sslopt,
            )

            self.ws.send(json.dumps({
                "msg": "connect",
                "version": "1",
                "support": ["1"],
            }))

            handshake = json.loads(self.ws.recv())
            if handshake.get("msg") != "connected":
                raise ZfsError(f"Middleware handshake failed: {handshake}")

            login_id = self._next_id()
            self.ws.send(json.dumps({
                "id": login_id,
                "msg": "method",
                "method": "auth.login_with_api_key",
                "params": [settings.TRUENAS_API_KEY],
            }))

            resp = json.loads(self.ws.recv())
            if resp.get("error"):
                raise ZfsError(f"TrueNAS auth error: {resp}")
            if resp.get("result") is not True:
                raise ZfsError(f"TrueNAS auth rejected: {resp}")

        except Exception as e:
            raise ZfsError(str(e))

    def call(self, method, *params):
        try:
            req_id = self._next_id()
            self.ws.send(json.dumps({
                "id": req_id,
                "msg": "method",
                "method": method,
                "params": list(params),
            }))
            resp = json.loads(self.ws.recv())
            if resp.get("error"):
                raise ZfsError(resp["error"])
            return resp.get("result")
        except Exception as e:
            raise ZfsError(str(e))

    def subscribe(self, collection, sub_id):
        self.ws.send(json.dumps({
            "id": self._next_id(),
            "msg": "sub",
            "name": collection,
            "id": sub_id,
        }))

    def unsubscribe(self, sub_id):
        self.ws.send(json.dumps({
            "id": self._next_id(),
            "msg": "unsub",
            "id": sub_id,
        }))

    def recv(self):
        return json.loads(self.ws.recv())

    def close(self):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass


def list_snapshots(dataset=None):
    client = TrueNASMiddlewareClient()
    try:
        client.connect()
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
        client.close()


def list_datasets_with_snapshots():
    client = TrueNASMiddlewareClient()
    try:
        client.connect()
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
        client.close()


def rollback_snapshot(dataset, snapshot):
    client = TrueNASMiddlewareClient()
    try:
        client.connect()
        return client.call("zfs.snapshot.rollback", f"{dataset}@{snapshot}")
    finally:
        client.close()


def clone_snapshot(dataset, snapshot, target):
    client = TrueNASMiddlewareClient()
    try:
        client.connect()
        return client.call("zfs.snapshot.clone", f"{dataset}@{snapshot}", target)
    finally:
        client.close()


def get_job(job_id):
    client = TrueNASMiddlewareClient()
    try:
        client.connect()
        return client.call("core.get_jobs", [["id", "=", job_id]])
    finally:
        client.close()


def get_pools_health():
    client = TrueNASMiddlewareClient()
    try:
        client.connect()
        pools = client.call("pool.query") or []
        return {p.get("name"): p.get("status", "UNKNOWN") for p in pools}
    finally:
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


def snapshot_diff(dataset, a, b):
    client = TrueNASMiddlewareClient()
    try:
        client.connect()
        result = client.call(
            "zfs.snapshot.get_diff",
            f"{dataset}@{a}",
            f"{dataset}@{b}",
        )
        return result or {"added": [], "removed": [], "modified": []}
    finally:
        client.close()


def list_snapshot_files(dataset, snapshot, path=""):
    import os

    # Prevent leading slashes from causing os.path.join to ignore base_path
    path = path.lstrip("/")

    base_path = os.path.join(
        "/data",
        dataset,
        ".zfs",
        "snapshot",
        snapshot,
    )

    # Normalize incoming subpath: treat empty or '/' as the snapshot root,
    # and strip any leading slashes so os.path.join doesn't ignore base_path.
    if not path or path == "/":
        rel_path = ""
    else:
        rel_path = path.lstrip("/")

    safe_path = os.path.normpath(os.path.join(base_path, rel_path))

    # Resolve real paths and ensure containment using commonpath to avoid
    # directory escape via symlinks or ../ segments.
    base_real = os.path.realpath(base_path)
    safe_real = os.path.realpath(safe_path)

    try:
        common = os.path.commonpath([base_real, safe_real])
    except Exception:
        raise ZfsError("Invalid snapshot path")

    if common != base_real:
        raise ZfsError("Invalid snapshot path")

    if not os.path.exists(safe_real):
        raise ZfsError(f"Snapshot path does not exist: {safe_real}")

    entries = []
    try:
        names = os.listdir(safe_real)
    except OSError as e:
        raise ZfsError(f"Unable to list snapshot path: {e}")

    for name in names:
        full_real = os.path.join(safe_real, name)
        entries.append({
            "name": name,
            "is_dir": os.path.isdir(full_real),
            "size": os.path.getsize(full_real) if os.path.isfile(full_real) else None,
        })

    entries.sort(key=lambda x: (not x["is_dir"], x["name"].lower()))

    return entries