import os

from config import is_configured
from services.base_service import BaseZFSService
from services.generic_zfs_service import GenericZFSService
from services.truenas_service import TrueNASZFSService


def _resolve_backend_name() -> str:
    forced = (os.getenv("FORCE_BACKEND") or "").strip().lower()
    if forced:
        if forced not in {"truenas", "generic"}:
            raise RuntimeError(f"Invalid FORCE_BACKEND value: {forced!r}")
        return forced
    return "truenas" if is_configured() else "generic"


def get_service() -> BaseZFSService:
    backend = _resolve_backend_name()
    if backend == "truenas":
        service = TrueNASZFSService()
    elif backend == "generic":
        service = GenericZFSService()
    else:
        raise RuntimeError(f"Unsupported backend: {backend!r}")

    if not isinstance(service, BaseZFSService):
        raise RuntimeError(f"Invalid backend service type: {type(service)!r}")

    return service
