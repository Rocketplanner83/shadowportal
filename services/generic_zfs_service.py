from services.base_service import BaseZFSService


class GenericZFSService(BaseZFSService):
    def list_datasets(self):
        raise NotImplementedError("Generic backend list_datasets is not implemented")

    def list_snapshots(self, dataset):
        raise NotImplementedError("Generic backend list_snapshots is not implemented")

    def list_snapshot_files(self, dataset, snapshot, path=""):
        raise NotImplementedError("Generic backend list_snapshot_files is not implemented")

    def restore_path(self, dataset, snapshot, subpath, target_container_path, overwrite=False):
        raise NotImplementedError("Generic backend restore_path is not implemented")

    def snapshot_diff(self, dataset, a, b):
        raise NotImplementedError("Generic backend snapshot_diff is not implemented")

    def capabilities(self) -> dict[str, bool]:
        caps = super().capabilities()
        caps["jobs"] = False
        return caps
