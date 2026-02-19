from typing import Any
import os
import time
from datetime import datetime, timezone

from utils import zfs as lowlevel
from utils.paths import container_to_host_path


def build_pool_tree(datasets):
    """
    Build structured pool tree from flat dataset list.

    datasets: list of dataset dicts from list_datasets()

    Returns:
        [
            {
                "name": "dagda",
                "datasets": [
                    { dataset_dict },
                    ...
                ]
            },
            ...
        ]
    """
    pools = {}

    for ds in datasets:
        name = ds.get("name")
        if not name:
            continue

        pool_name = name.split("/", 1)[0]
        pools.setdefault(pool_name, []).append(ds)

    return [
        {
            "name": pool,
            "datasets": sorted(
                pools[pool],
                key=lambda d: d.get("name", "").lower()
            )
        }
        for pool in sorted(pools.keys())
    ]


class ZfsService:
    """High-level ZFS/TrueNAS service wrapper used by the application.

    This class delegates to low-level functions in `utils.zfs` and expects
    the caller to provide an already-connected request-scoped client.
    """

    def __init__(self) -> None:
        # simple in-memory cache for snapshot lists: {dataset: (ts, snaps)}
        self._snapshot_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._snapshot_ttl = 30.0

    def _require_client(self, client: lowlevel.TrueNASClient | None) -> lowlevel.TrueNASClient:
        if client is None:
            raise RuntimeError("Request-scoped TrueNAS client is required")
        return client

    def validate_connectivity(self, client: lowlevel.TrueNASClient | None) -> None:
        c = self._require_client(client)
        c.call("system.version")

    def list_datasets_with_snapshots(self, client: lowlevel.TrueNASClient | None) -> dict[str, Any]:
        c = self._require_client(client)
        return lowlevel.list_datasets_with_snapshots(client=c)

    def list_snapshots(self, dataset: str | None = None, client: lowlevel.TrueNASClient | None = None) -> list[dict[str, Any]]:
        c = self._require_client(client)
        return lowlevel.list_snapshots(dataset, client=c)

    def rollback_snapshot(self, dataset: str, snapshot: str, client: lowlevel.TrueNASClient | None = None) -> Any:
        c = self._require_client(client)
        res = lowlevel.rollback_snapshot(dataset, snapshot, client=c)
        # snapshot set may have changed; invalidate cache for this dataset
        try:
            self._snapshot_cache.pop(dataset, None)
        except Exception:
            pass
        return res

    def clone_snapshot(self, dataset: str, snapshot: str, target: str, client: lowlevel.TrueNASClient | None = None) -> Any:
        c = self._require_client(client)
        res = lowlevel.clone_snapshot(dataset, snapshot, target, client=c)
        # Invalidate cache for the target dataset and source dataset
        try:
            self._snapshot_cache.pop(dataset, None)
        except Exception:
            pass
        try:
            self._snapshot_cache.pop(target, None)
        except Exception:
            pass
        return res

    def get_job(self, job_id: int, client: lowlevel.TrueNASClient | None = None) -> Any:
        c = self._require_client(client)
        return lowlevel.get_job(job_id, client=c)

    def get_pools_health(self, client: lowlevel.TrueNASClient | None = None) -> dict[str, str]:
        c = self._require_client(client)
        return lowlevel.get_pools_health(client=c)

    def get_dataset_objects(self, client: lowlevel.TrueNASClient | None = None) -> list[dict[str, Any]]:
        c = self._require_client(client)
        return c.call("zfs.dataset.query") or []

    def list_datasets(self, client: lowlevel.TrueNASClient | None = None) -> list[dict[str, Any]]:
        """Return raw dataset objects (alias for get_dataset_objects)."""
        return self.get_dataset_objects(client=client)

    def build_pool_tree(self, datasets: list[dict[str, Any]], client: lowlevel.TrueNASClient | None = None) -> list[dict[str, Any]]:
        """Build pools and annotate datasets with snapshot counts.

        Fetch snapshots once and compute per-dataset metadata in-memory to
        avoid opening many websocket calls during a single page render.
        """
        c = self._require_client(client)
        pools = {}
        snapshot_meta: dict[str, dict[str, Any]] = {}

        try:
            now = time.time()
            cached = self._snapshot_cache.get("__all__")
            if cached and (now - cached[0]) < self._snapshot_ttl:
                all_snaps = cached[1]
            else:
                all_snaps = self.list_snapshots(client=c) or []
                self._snapshot_cache["__all__"] = (now, all_snaps)

            for snap in all_snaps:
                dataset_name = snap.get("dataset")
                if not dataset_name:
                    continue

                created = snap.get("created_at") or datetime.min.replace(tzinfo=timezone.utc)
                item = snapshot_meta.get(dataset_name)
                if item is None:
                    snapshot_meta[dataset_name] = {
                        "count": 1,
                        "latest_created": created,
                        "latest_name": snap.get("snapshot_name"),
                    }
                    continue

                item["count"] += 1
                if created > item["latest_created"]:
                    item["latest_created"] = created
                    item["latest_name"] = snap.get("snapshot_name")
        except Exception:
            snapshot_meta = {}

        for ds in datasets:
            name = ds.get("name")
            if not name:
                continue
            ds_copy = dict(ds)
            meta = snapshot_meta.get(name)
            if not meta:
                ds_copy["snapshot_count"] = 0
                ds_copy["latest_snapshot"] = None
            else:
                ds_copy["snapshot_count"] = meta.get("count", 0)
                ds_copy["latest_snapshot"] = meta.get("latest_name")

            pool_name = name.split("/", 1)[0]
            pools.setdefault(pool_name, []).append(ds_copy)

        return [
            {
                "name": pool,
                "datasets": sorted(pools[pool], key=lambda d: d.get("name", "").lower()),
            }
            for pool in sorted(pools.keys())
        ]

    def restore_path(
        self,
        dataset: str,
        snapshot: str,
        subpath: str,
        target_container_path: str,
        *,
        overwrite: bool = False,
        client: lowlevel.TrueNASClient | None = None,
    ) -> int:
        """Schedule a middlewared job to copy from a snapshot path into the live dataset.

        Parameters:
          - dataset: full dataset name (no splitting)
          - snapshot: snapshot name
          - subpath: relative path inside snapshot (may be empty)
          - target_container_path: destination container path (e.g. /data/<dataset>/...)

        Returns the middleware job id (int) or raises on error.
        """
        c = self._require_client(client)

        # build container source path
        sub = (subpath or "").lstrip("/")
        src_container_path = os.path.normpath(os.path.join("/data", dataset, ".zfs", "snapshot", snapshot, sub))

        if ".zfs/snapshot/" not in src_container_path:
            raise ValueError("Source must be inside a .zfs/snapshot path")
        if ".zfs/snapshot" in target_container_path:
            raise ValueError("Destination must not be inside a snapshot path")

        src_host = container_to_host_path(src_container_path)
        dest_host = container_to_host_path(target_container_path)

        result = c.call("filesystem.copy", src_host, dest_host, {"recursive": True, "preserve": True, "overwrite": overwrite})
        job_id = None
        if isinstance(result, dict) and "id" in result:
            job_id = result.get("id")
        elif isinstance(result, int):
            job_id = result
        else:
            try:
                job_id = int(result)
            except Exception:
                job_id = None

        if job_id is None:
            raise RuntimeError(f"middleware returned unexpected result for filesystem.copy: {result}")

        return job_id

    def get_dataset_space(self, dataset_objects: list[dict[str, Any]]) -> dict[str, Any]:
        return lowlevel.get_dataset_space(dataset_objects)

    def snapshot_diff(self, dataset: str, a: str, b: str, client: lowlevel.TrueNASClient | None = None) -> dict[str, Any]:
        c = self._require_client(client)
        return lowlevel.snapshot_diff(dataset, a, b, client=c)

    def list_snapshot_files(
        self,
        dataset: str,
        snapshot: str,
        path: str = "",
        client: lowlevel.TrueNASClient | None = None,
    ) -> list[dict[str, Any]]:
        c = self._require_client(client)
        return lowlevel.list_snapshot_files(dataset, snapshot, path, client=c)
