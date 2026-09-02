# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from unittest.mock import MagicMock, patch

import pytest
import requests
import yaml

from src.config.loader import load_yaml_config
from src.utils.ingest import calculate_sha256, download_stream

# -----------------------------------------------------------------------------
# load_yaml
# -----------------------------------------------------------------------------

def test_load_yaml_config_success(tmp_path):
    # Setup: Create a valid YAML file
    config_file = tmp_path / "config.yaml"
    config_file.write_text("batch_size: 32\nenvironment: 'test'")

    # Execute
    raw_text, result = load_yaml_config(config_file)

    # Assert
    assert raw_text == "batch_size: 32\nenvironment: 'test'"
    assert result == {"batch_size": 32, "environment": "test"}


def test_load_yaml_config_empty_file(tmp_path):
    # Setup: Create an empty YAML file
    empty_file = tmp_path / "empty.yaml"
    empty_file.write_text("")

    # Execute
    raw_text, result = load_yaml_config(empty_file)

    assert raw_text == ""
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


# -----------------------------------------------------------------------------
# calculate_sha256
# -----------------------------------------------------------------------------

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
        calculate_sha256(file)
        # should raise error and end here


# -----------------------------------------------------------------------------
# download_stream
# -----------------------------------------------------------------------------

@patch("src.utils.ingest.requests.get")
def test_download_stream_success(mock_get, tmp_path):
    output_file = tmp_path / "downloaded_data.bin"
    fake_url = "https://example.com/dataset.zip"
    
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"chunk1_", b"chunk2"]
    mock_get.return_value.__enter__.return_value = mock_response

    download_stream(fake_url, output_file, timeout=10)

    mock_get.assert_called_once_with(fake_url, stream=True, timeout=10)
    assert output_file.exists()
    assert output_file.read_bytes() == b"chunk1_chunk2"


@patch("src.utils.ingest.requests.get")
def test_download_stream_http_error_cleans_up_temp_file(mock_get, tmp_path):
    output_file = tmp_path / "failed_download.bin"
    temp_file = output_file.with_suffix(output_file.suffix + ".part")
    fake_url = "https://example.com/404.zip"

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mock_get.return_value.__enter__.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError, match="404 Not Found"):
        download_stream(fake_url, output_file)

    mock_get.assert_called_once_with(fake_url, stream=True, timeout=15.0)
    assert not temp_file.exists()
    assert not output_file.exists()


@patch("src.utils.ingest.requests.get")
def test_download_stream_invalid_output_directory(mock_get, tmp_path):
    invalid_path = tmp_path / "missing_folder" / "data.csv"
    fake_url = "https://example.com/data.csv"

    mock_response = MagicMock()
    mock_get.return_value.__enter__.return_value = mock_response

    with pytest.raises(FileNotFoundError):
        download_stream(fake_url, invalid_path)

    mock_get.assert_called_once_with(fake_url, stream=True, timeout=15.0)
    assert not invalid_path.exists()


@pytest.mark.parametrize("exception", [
    requests.exceptions.Timeout("timed out"),
    requests.exceptions.ConnectionError("connection refused"),
])
@patch("src.utils.ingest.requests.get")
def test_download_stream_request_level_errors_clean_up(mock_get, tmp_path, exception):
    output_file = tmp_path / "data.bin"
    temp_file = output_file.with_suffix(output_file.suffix + ".part")
    fake_url = "https://example.com/data.bin"

    mock_get.side_effect = exception

    with pytest.raises(type(exception)):
        download_stream(fake_url, output_file)

    assert not temp_file.exists()
    assert not output_file.exists()


@patch("src.utils.ingest.requests.get")
def test_download_stream_mid_stream_failure_cleans_up(mock_get, tmp_path):
    output_file = tmp_path / "data.bin"
    temp_file = output_file.with_suffix(output_file.suffix + ".part")
    fake_url = "https://example.com/data.bin"

    def failing_chunks(chunk_size):
        yield b"chunk1"
        raise requests.exceptions.ChunkedEncodingError("Stream interrupted")

    mock_response = MagicMock()
    mock_response.iter_content.side_effect = failing_chunks
    mock_get.return_value.__enter__.return_value = mock_response

    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        download_stream(fake_url, output_file)

    assert not temp_file.exists()
    assert not output_file.exists()
