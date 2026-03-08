from __future__ import annotations

import json
from pathlib import Path

from data_agent_core.analytics.models import RegisteredDataset


class DatasetRegistry:
    def __init__(self, state_root: str | Path = ".state") -> None:
        self.state_root = Path(state_root)
        self.datasets_file = self.state_root / "datasets.json"
        self.semantics_dir = self.state_root / "semantics"
        self.profiles_dir = self.state_root / "profiles"
        self.evidence_dir = self.state_root / "evidence"
        self._ensure_layout()

    def register_dataset(self, dataset: RegisteredDataset) -> None:
        datasets = {item.dataset_name: item for item in self.list_datasets()}
        datasets[dataset.dataset_name] = dataset
        payload = [item.model_dump(mode="json") for item in sorted(datasets.values(), key=lambda x: x.dataset_name)]
        self.datasets_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def get_dataset(self, dataset_name: str) -> RegisteredDataset:
        for item in self.list_datasets():
            if item.dataset_name == dataset_name:
                return item
        raise KeyError(f"Dataset not found: {dataset_name}")

    def list_datasets(self) -> list[RegisteredDataset]:
        if not self.datasets_file.exists():
            return []
        raw = json.loads(self.datasets_file.read_text(encoding="utf-8"))
        return [RegisteredDataset.model_validate(item) for item in raw]

    def update_semantic_path(self, dataset_name: str, semantic_path: str) -> None:
        datasets = {item.dataset_name: item for item in self.list_datasets()}
        dataset = datasets.get(dataset_name)
        if dataset is None:
            raise KeyError(f"Dataset not found: {dataset_name}")
        dataset.semantic_path = semantic_path
        self.register_dataset(dataset)

    def update_profile_path(self, dataset_name: str, profile_path: str) -> None:
        datasets = {item.dataset_name: item for item in self.list_datasets()}
        dataset = datasets.get(dataset_name)
        if dataset is None:
            raise KeyError(f"Dataset not found: {dataset_name}")
        dataset.profile_path = profile_path
        self.register_dataset(dataset)

    def _ensure_layout(self) -> None:
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.semantics_dir.mkdir(parents=True, exist_ok=True)
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        if not self.datasets_file.exists():
            self.datasets_file.write_text("[]", encoding="utf-8")
