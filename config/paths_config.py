#!/usr/bin/env python3
"""
Minimale Pfad-Konfiguration für json-database (Standalone-Repo)
"""

import os
from pathlib import Path
from typing import List


class ProjectPaths:
    _project_root: Path = None

    @classmethod
    def _initialize_root(cls) -> Path:
        if cls._project_root is None:
            cls._project_root = Path(__file__).resolve().parent.parent
        return cls._project_root

    @classmethod
    def project_root(cls) -> Path:
        return cls._initialize_root()

    @classmethod
    def config_directory(cls) -> Path:
        shared = cls.project_root() / "config" / "shared" / "config"
        return shared if shared.exists() else cls.project_root() / "config"

    @classmethod
    def dynamic_system_outputs_directory(cls) -> Path:
        return cls.project_root() / "dynamic_system_outputs"

    # OUTBOX (wichtig: JSON-DB importiert aus BL-Outbox)
    @classmethod
    def outbox_directory(cls) -> Path:
        env_root = os.environ.get("OUTBOX_ROOT")
        if env_root:
            try:
                p = Path(env_root).resolve()
                p.mkdir(parents=True, exist_ok=True)
                return p
            except Exception:
                pass
        return cls.dynamic_system_outputs_directory() / "outbox"

    @classmethod
    def outbox_churn_experiment_directory(cls, experiment_id: int) -> Path:
        return (cls.outbox_directory() / "churn") / f"experiment_{int(experiment_id)}"

    @classmethod
    def outbox_cox_experiment_directory(cls, experiment_id: int) -> Path:
        return (cls.outbox_directory() / "cox") / f"experiment_{int(experiment_id)}"

    @classmethod
    def outbox_counterfactuals_directory(cls) -> Path:
        return cls.outbox_directory() / "counterfactuals"

    # Konfigurationsdateien
    @classmethod
    def data_dictionary_file(cls) -> Path:
        return cls.config_directory() / "data_dictionary_optimized.json"

    @classmethod
    def feature_mapping_file(cls) -> Path:
        return cls.config_directory() / "feature_mapping.json"

    @classmethod
    def cf_cost_policy_file(cls) -> Path:
        return cls.config_directory() / "cf_cost_policy.json"

    # Utilities
    @classmethod
    def ensure_directory_exists(cls, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        return directory


