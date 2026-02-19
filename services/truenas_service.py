from services.base_service import BaseZFSService
from services.zfs_service import ZfsService


class TrueNASZFSService(ZfsService, BaseZFSService):
    def capabilities(self) -> dict[str, bool]:
        caps = super().capabilities()
        caps["jobs"] = True
        return caps
