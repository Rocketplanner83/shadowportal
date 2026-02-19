from abc import ABC, abstractmethod


class BaseZFSService(ABC):
    def capabilities(self) -> dict[str, bool]:
        return {
            "diff": True,
            "restore": True,
            "jobs": False,
        }

    @abstractmethod
    def list_datasets(self):
        raise NotImplementedError

    @abstractmethod
    def list_snapshots(self, dataset):
        raise NotImplementedError

    @abstractmethod
    def list_snapshot_files(self, dataset, snapshot, path=""):
        raise NotImplementedError

    @abstractmethod
    def restore_path(self, dataset, snapshot, subpath, target_container_path, overwrite=False):
        raise NotImplementedError

    @abstractmethod
    def snapshot_diff(self, dataset, a, b):
        raise NotImplementedError

    def validate_connectivity(self, client=None):
        raise NotImplementedError("validate_connectivity is not implemented for this backend")

    def build_pool_tree(self, datasets, client=None):
        raise NotImplementedError("build_pool_tree is not implemented for this backend")

    def rollback_snapshot(self, dataset, snapshot, client=None):
        raise NotImplementedError("rollback_snapshot is not implemented for this backend")

    def clone_snapshot(self, dataset, snapshot, target, client=None):
        raise NotImplementedError("clone_snapshot is not implemented for this backend")

    def get_job(self, job_id, client=None):
        raise NotImplementedError("get_job is not implemented for this backend")
