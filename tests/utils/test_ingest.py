# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import hashlib
from pathlib import Path

import pytest
import requests
import yaml
from src.utils.ingest import load_yaml_config
from src.utils.ingest import calculate_sha256
from src.utils.ingest import download_stream


def test_load_yaml_config_success(tmp_path):
    # Setup: Create a valid YAML file
    config_file = tmp_path / "config.yaml"
    config_file.write_text("batch_size: 32\nenvironment: 'test'")

    # Execute
    result = load_yaml_config(config_file)

    # Assert
    assert result == {"batch_size": 32, "environment": "test"}


def test_load_yaml_config_empty_file(tmp_path):
    # Setup: Create an empty YAML file
    empty_file = tmp_path / "empty.yaml"
    empty_file.write_text("")

    # Execute
    result = load_yaml_config(empty_file)

    assert result == {}


def test_load_yaml_config_missing_file(tmp_path):
    # Setup: Non-existent path
    missing_file = tmp_path / "non_existent_folder" / "config.yaml"

    # Execute & Assert
    with pytest.raises(FileNotFoundError):
        load_yaml_config(missing_file)


def test_load_yaml_config_invalid_syntax(tmp_path):
    # Setup: Create malformed YAML
    bad_yaml_file = tmp_path / "bad_syntax.yaml"
    bad_yaml_file.write_text("key: : invalid_yaml")

    # Execute & Assert
    with pytest.raises(yaml.YAMLError):
        load_yaml_config(bad_yaml_file)









def test_calculate_sha256_working(tmp_path):
    # test case 1 correct output testing
    file = tmp_path / "sample.txt"
    file.write_bytes(b"test text")
    result = calculate_sha256(file)
    assert result == "0f46738ebed370c5c52ee0ad96dec8f459fb901c2ca4e285211eddf903bf1598"

def test_calculate_sha256_empty_file(tmp_path):
    # test case 2 empty file testing
    file = tmp_path / "empty.txt"
    file.write_bytes(b"")
    result = calculate_sha256(file)
    assert result == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

def test_calculate_sha256_no_file(tmp_path):
    # test case 3 no file testing
    file = tmp_path / "nothing.txt"
    with pytest.raises(FileNotFoundError):
        result = calculate_sha256(file)
        # should raise error and end here





def test_download_stream_success()
