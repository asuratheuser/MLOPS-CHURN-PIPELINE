"""Tests for src.storage.storage:
get_project_root, ensure_dir_exists, get_raw_data_path.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

from src.storage.storage import get_project_root, ensure_dir_exists, get_raw_data_path


# ─────────────────────────────────────────────────────────────────
# testing get_project_root
# ─────────────────────────────────────────────────────────────────

class TestGetProjectRoot:

    def test_finds_anchor_file_in_immediate_parent(self, tmp_path):
        (tmp_path / "requirements.txt").touch()
        fake_module = tmp_path / "src" / "storage" / "storage.py"
        fake_module.parent.mkdir(parents=True)

        result = get_project_root(start=fake_module)

        assert result == tmp_path

    def test_finds_anchor_file_several_levels_up(self, tmp_path):
        (tmp_path / "requirements.txt").touch()
        fake_module = tmp_path / "a" / "b" / "c" / "d" / "storage.py"
        fake_module.parent.mkdir(parents=True)

        result = get_project_root(start=fake_module)

        assert result == tmp_path

    def test_returns_nearest_match_when_multiple_anchors_exist(self, tmp_path):
        (tmp_path / "requirements.txt").touch()
        inner = tmp_path / "proj"
        inner.mkdir()
        (inner / "requirements.txt").touch()
        fake_module = inner / "src" / "storage" / "storage.py"
        fake_module.parent.mkdir(parents=True)

        result = get_project_root(start=fake_module)

        assert result == inner

    def test_custom_anchor_file(self, tmp_path):
        (tmp_path / "pyproject.toml").touch()
        fake_module = tmp_path / "src" / "storage" / "storage.py"
        fake_module.parent.mkdir(parents=True)

        result = get_project_root(anchor_file="pyproject.toml", start=fake_module)

        assert result == tmp_path

    def test_raises_when_anchor_not_found(self, tmp_path):
        fake_module = tmp_path / "src" / "storage" / "storage.py"
        fake_module.parent.mkdir(parents=True)

        with pytest.raises(FileNotFoundError, match="requirements.txt"):
            get_project_root(start=fake_module)

    def test_raises_with_custom_anchor_name_in_message(self, tmp_path):
        fake_module = tmp_path / "src" / "storage" / "storage.py"
        fake_module.parent.mkdir(parents=True)

        with pytest.raises(FileNotFoundError, match="setup.py"):
            get_project_root(anchor_file="setup.py", start=fake_module)

    def test_default_start_uses_real_file_and_resolves(self):
        # sanity check against your actual checked-out project —
        # requirements.txt genuinely sits at your real project root
        result = get_project_root()

        assert result.is_dir()
        assert (result / "requirements.txt").exists()

    def test_start_accepts_relative_path_and_resolves_it(self, tmp_path, monkeypatch):
        (tmp_path / "requirements.txt").touch()
        fake_module = tmp_path / "src" / "storage" / "storage.py"
        fake_module.parent.mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        result = get_project_root(start=Path("src/storage/storage.py"))

        assert result == tmp_path.resolve()


# ─────────────────────────────────────────────────────────────────
# testing ensure_dir_exists
# ─────────────────────────────────────────────────────────────────

class TestEnsureDirExists:

    def test_creates_new_directory(self, tmp_path):
        target = tmp_path / "new_folder"

        result = ensure_dir_exists(target)

        assert target.is_dir()
        assert result == target

    def test_creates_nested_directories(self, tmp_path):
        target = tmp_path / "a" / "b" / "c"

        result = ensure_dir_exists(target)

        assert target.is_dir()
        assert result == target

    def test_is_idempotent_on_existing_directory(self, tmp_path):
        target = tmp_path / "already_here"
        target.mkdir()

        result = ensure_dir_exists(target)

        assert target.is_dir()
        assert result == target

    def test_raises_if_path_is_a_file(self, tmp_path):
        target = tmp_path / "im_a_file.txt"
        target.touch()

        with pytest.raises(NotADirectoryError):
            ensure_dir_exists(target)

    def test_returns_same_type_as_input(self, tmp_path):
        target = tmp_path / "typed_check"

        result = ensure_dir_exists(target)

        assert isinstance(result, Path)


# ─────────────────────────────────────────────────────────────────
# testing get_raw_data_path
# ─────────────────────────────────────────────────────────────────

class TestGetRawDataPath:

    @patch("src.storage.storage.get_project_root")
    def test_builds_correct_path_and_creates_directory(self, mock_root, tmp_path):
        mock_root.return_value = tmp_path

        result = get_raw_data_path("dataset.csv")

        assert result == tmp_path / "data" / "raw" / "dataset.csv"
        assert (tmp_path / "data" / "raw").is_dir()

    @patch("src.storage.storage.get_project_root")
    def test_empty_filename_returns_directory_path(self, mock_root, tmp_path):
        mock_root.return_value = tmp_path

        result = get_raw_data_path()

        assert result == tmp_path / "data" / "raw"
        assert result.is_dir()

    @patch("src.storage.storage.get_project_root")
    def test_directory_created_even_when_filename_given(self, mock_root, tmp_path):
        mock_root.return_value = tmp_path

        result = get_raw_data_path("not_yet_downloaded.bin")

        assert not result.exists()
        assert result.parent.is_dir()

    @patch("src.storage.storage.get_project_root")
    def test_propagates_project_root_not_found(self, mock_root):
        mock_root.side_effect = FileNotFoundError("Could not locate project root")

        with pytest.raises(FileNotFoundError):
            get_raw_data_path("dataset.csv")

    @patch("src.storage.storage.get_project_root")
    def test_idempotent_across_multiple_calls(self, mock_root, tmp_path):
        mock_root.return_value = tmp_path

        first = get_raw_data_path("a.csv")
        second = get_raw_data_path("b.csv")

        assert first.parent == second.parent
        assert first.parent.is_dir()

    @patch("src.storage.storage.get_project_root")
    def test_creates_raw_dir_fresh_when_it_does_not_exist_yet(self, mock_root, tmp_path):
        # explicitly confirms the "should create fresh" requirement —
        # data/ itself doesn't exist yet, not just data/raw/
        mock_root.return_value = tmp_path
        assert not (tmp_path / "data").exists()

        get_raw_data_path("dataset.csv")

        assert (tmp_path / "data" / "raw").is_dir()