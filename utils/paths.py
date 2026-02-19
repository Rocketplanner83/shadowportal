import os
from typing import Tuple
from utils.zfs import ZfsError


def _real_under(root: str, path: str) -> bool:
    root_real = os.path.realpath(root)
    path_real = os.path.realpath(path)
    return path_real == root_real or path_real.startswith(root_real + os.sep)


def container_to_host_path(p: str) -> str:
    """Map container path (/data/...) to host path (/mnt/...)."""
    if p.startswith("/data/"):
        return "/mnt/" + p[len("/data/"):]
    return p


def host_to_container_path(p: str) -> str:
    if p.startswith("/mnt/"):
        return "/data/" + p[len("/mnt/"):]
    return p


def validate_container_restore_paths(src_container: str, dst_container: str):
    """Ensure source and destination are under /data and source is from a snapshot."""
    if ".zfs/snapshot/" not in src_container:
        raise ZfsError("Source must be inside a .zfs/snapshot path")
    if ".zfs/snapshot" in dst_container:
        raise ZfsError("Destination must not be inside a snapshot path")

    if not _real_under("/data", src_container) or not _real_under("/data", dst_container):
        raise ZfsError("Restore paths must reside under /data")

    return True
