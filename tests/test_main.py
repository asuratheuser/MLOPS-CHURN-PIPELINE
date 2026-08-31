"""Tests for the application entry point and configuration boundary."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import main


def write_config(path: Path, overrides: str = "") -> Path:
    """Write a valid minimal v1 config, optionally extended by YAML overrides."""
    path.write_text(
        """\
pipeline_name: test-pipeline
dataset:
  name: churn
  url: https://example.com/churn.csv
  filename: churn.csv
  expected_sha256: 16320c9c1ec72448db59aa0a26a0b95401046bef5d02fd3aeb906448e3055e91
schema:
  target_column: Churn
  required_columns: [customerID, Churn]
split:
  train_size: 0.7
  validation_size: 0.1
  test_size: 0.2
  random_seed: 42
  stratify: true
training:
  model_name: logistic_regression
  random_seed: 42
artifacts:
  directory: artifacts/runs
"""
        + overrides,
        encoding="utf-8",
    )
    return path


class TestLoadConfig:
    def test_loads_and_validates_valid_yaml(self, tmp_path: Path) -> None:
        config_path = write_config(tmp_path / "pipeline.yaml")

        config, digest = main.load_config(config_path)

        assert config.pipeline_name == "test-pipeline"
        assert config.schema_config.target_column == "Churn"
        assert len(digest) == 64

    def test_rejects_missing_config_file(self, tmp_path: Path) -> None:
        with pytest.raises(main.ConfigurationError, match="not found"):
            main.load_config(tmp_path / "missing.yaml")

    def test_rejects_non_yaml_file(self, tmp_path: Path) -> None:
        config_path = write_config(tmp_path / "pipeline.txt")

        with pytest.raises(main.ConfigurationError, match=".yaml or .yml"):
            main.load_config(config_path)

    def test_rejects_non_mapping_yaml_root(self, tmp_path: Path) -> None:
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text("- not\n- a mapping\n", encoding="utf-8")

        with pytest.raises(main.ConfigurationError, match="root must be a mapping"):
            main.load_config(config_path)

    def test_rejects_unknown_configuration_key(self, tmp_path: Path) -> None:
        config_path = write_config(tmp_path / "pipeline.yaml", "unknown_setting: true\n")

        with pytest.raises(main.ConfigurationError, match="Extra inputs are not permitted"):
            main.load_config(config_path)

    def test_rejects_invalid_split_total(self, tmp_path: Path) -> None:
        config_path = write_config(
            tmp_path / "pipeline.yaml",
            "# The duplicate key replaces the earlier split section.\n"
            "split:\n  train_size: 0.7\n  validation_size: 0.2\n  test_size: 0.2\n  random_seed: 42\n",
        )

        with pytest.raises(main.ConfigurationError, match="must sum to 1.0"):
            main.load_config(config_path)


class TestProjectPaths:
    def test_resolves_project_relative_path(self, tmp_path: Path) -> None:
        assert main.resolve_project_path(tmp_path, Path("artifacts/runs")) == tmp_path / "artifacts/runs"

    def test_rejects_path_outside_project_root(self, tmp_path: Path) -> None:
        with pytest.raises(main.ConfigurationError, match="escapes the project root"):
            main.resolve_project_path(tmp_path, Path("../outside"))


class TestRunContext:
    def test_creates_artifact_directory_and_provenance_files(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        config_path = write_config(tmp_path / "pipeline.yaml")
        config, digest = main.load_config(config_path)
        monkeypatch.setattr(main, "get_git_commit", lambda _: "abc123")

        context = main.create_run_context(config, tmp_path, config_path, digest)

        assert context.artifact_dir.is_dir()
        assert (context.artifact_dir / "pipeline.yaml").read_text(encoding="utf-8") == config_path.read_text(encoding="utf-8")
        metadata = json.loads((context.artifact_dir / "run_metadata.json").read_text(encoding="utf-8"))
        assert metadata["run_id"] == context.run_id
        assert metadata["config_sha256"] == digest
        assert metadata["git_commit"] == "abc123"


class TestMain:
    def test_returns_zero_when_pipeline_completes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        config_path = write_config(tmp_path / "pipeline.yaml")
        monkeypatch.setattr(main, "get_project_root", lambda: tmp_path)
        monkeypatch.setattr(main, "get_git_commit", lambda _: None)
        monkeypatch.setattr(main, "run_pipeline", lambda config, context: {"status": "success"})

        exit_code = main.main(["--config", str(config_path)])

        assert exit_code == 0

    def test_returns_two_for_configuration_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(main, "get_project_root", lambda: tmp_path)

        exit_code = main.main(["--config", "missing.yaml"])

        assert exit_code == 2

    def test_returns_one_when_pipeline_raises(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        config_path = write_config(tmp_path / "pipeline.yaml")
        monkeypatch.setattr(main, "get_project_root", lambda: tmp_path)
        monkeypatch.setattr(main, "get_git_commit", lambda _: None)

        def fail_pipeline(config: main.PipelineConfig, context: main.RunContext) -> None:
            del config, context
            raise RuntimeError("training failed")

        monkeypatch.setattr(main, "run_pipeline", fail_pipeline)

        exit_code = main.main(["--config", str(config_path)])

        assert exit_code == 1
