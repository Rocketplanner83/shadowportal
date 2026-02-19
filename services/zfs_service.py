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

    This class delegates to the existing low-level functions in `utils.zfs`
    and provides a small helper to open a connected TrueNAS client when
    the caller needs to subscribe/receive messages.
    """

    def __init__(self) -> None:
        # simple in-memory cache for snapshot lists: {dataset: (ts, snaps)}
        self._snapshot_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._snapshot_ttl = 30.0

    def open_client(self) -> lowlevel.TrueNASMiddlewareClient:
        c = lowlevel.TrueNASMiddlewareClient()
        c.connect()
        return c

    def validate_connectivity(self) -> None:
        c = lowlevel.TrueNASMiddlewareClient()
        c.connect()
        c.close()

    def list_datasets_with_snapshots(self) -> dict[str, Any]:
        return lowlevel.list_datasets_with_snapshots()

    def list_snapshots(self, dataset: str | None = None) -> list[dict[str, Any]]:
        return lowlevel.list_snapshots(dataset)

    def rollback_snapshot(self, dataset: str, snapshot: str) -> Any:
        res = lowlevel.rollback_snapshot(dataset, snapshot)
        # snapshot set may have changed; invalidate cache for this dataset
        try:
            self._snapshot_cache.pop(dataset, None)
        except Exception:
            pass
        return res

    def clone_snapshot(self, dataset: str, snapshot: str, target: str) -> Any:
        res = lowlevel.clone_snapshot(dataset, snapshot, target)
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

    def get_job(self, job_id: int) -> Any:
        # Use the websocket middleware client via lowlevel helper
        return lowlevel.get_job(job_id)

    def get_pools_health(self) -> dict[str, str]:
        return lowlevel.get_pools_health()

    def get_dataset_objects(self) -> list[dict[str, Any]]:
        client = lowlevel.TrueNASMiddlewareClient()
        try:
            client.connect()
            return client.call("zfs.dataset.query") or []
        finally:
            client.close()

    def list_datasets(self) -> list[dict[str, Any]]:
        """Return raw dataset objects (alias for get_dataset_objects)."""
        return self.get_dataset_objects()

    def build_pool_tree(self, datasets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Build pools and annotate datasets with snapshot counts.

        This method will call `list_snapshots` per dataset to populate
        `snapshot_count` and `latest_snapshot` keys on each dataset dict.
        """
        pools = {}

        for ds in datasets:
            name = ds.get("name")
            if not name:
                continue
            ds_copy = dict(ds)
            # compute snapshot counts lazily; errors should not break page
            try:
                now = time.time()
                cached = self._snapshot_cache.get(name)
                if cached and (now - cached[0]) < self._snapshot_ttl:
                    snaps = cached[1]
                else:
                    snaps = self.list_snapshots(name) or []
                    self._snapshot_cache[name] = (now, snaps)

                ds_copy["snapshot_count"] = len(snaps)
                if snaps:
                    snaps.sort(key=lambda x: x.get("created_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
                    ds_copy["latest_snapshot"] = snaps[0].get("snapshot_name")
                else:
                    ds_copy["latest_snapshot"] = None
            except Exception:
                ds_copy["snapshot_count"] = 0
                ds_copy["latest_snapshot"] = None

            pool_name = name.split("/", 1)[0]
            pools.setdefault(pool_name, []).append(ds_copy)

        return [
            {
                "name": pool,
                "datasets": sorted(pools[pool], key=lambda d: d.get("name", "").lower()),
            }
            for pool in sorted(pools.keys())
        ]

    def restore_path(self, dataset: str, snapshot: str, subpath: str, target_container_path: str, *, overwrite: bool = False) -> int:
        """Schedule a middlewared job to copy from a snapshot path into the live dataset.

        Parameters:
          - dataset: full dataset name (no splitting)
          - snapshot: snapshot name
          - subpath: relative path inside snapshot (may be empty)
          - target_container_path: destination container path (e.g. /data/<dataset>/...)

        Returns the middleware job id (int) or raises on error.
        """
        # build container source path
        sub = (subpath or "").lstrip("/")
        src_container_path = os.path.normpath(os.path.join("/data", dataset, ".zfs", "snapshot", snapshot, sub))

        if ".zfs/snapshot/" not in src_container_path:
            raise ValueError("Source must be inside a .zfs/snapshot path")
        if ".zfs/snapshot" in target_container_path:
            raise ValueError("Destination must not be inside a snapshot path")

        src_host = container_to_host_path(src_container_path)
        dest_host = container_to_host_path(target_container_path)

        client = lowlevel.TrueNASMiddlewareClient()
        try:
            client.connect()
            result = client.call("filesystem.copy", src_host, dest_host, {"recursive": True, "preserve": True, "overwrite": overwrite})
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
        finally:
            try:
                client.close()
            except Exception:
                pass

    def get_dataset_space(self, dataset_objects: list[dict[str, Any]]) -> dict[str, Any]:
        return lowlevel.get_dataset_space(dataset_objects)

    def snapshot_diff(self, dataset: str, a: str, b: str) -> dict[str, Any]:
        return lowlevel.snapshot_diff(dataset, a, b)

    def list_snapshot_files(self, dataset: str, snapshot: str, path: str = "") -> list[dict[str, Any]]:
        return lowlevel.list_snapshot_files(dataset, snapshot, path)
