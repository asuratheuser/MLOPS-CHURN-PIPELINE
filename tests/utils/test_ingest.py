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

def test_load_yaml_config(config_path : Path ) -> dict:
    """
    Opens a YAML file and converts its content into a Python dictionary.
    
    Input: config_path (Path) - Path object to the .yaml file
    Output: dict - Parsed dictionary of the YAML file
    """

    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, mode="r", encoding="utf-8") as file:
        config_data = yaml.safe_load(file)

    return config_data or {}


def test_load_yaml_config():
    # test case 1 for correct output

    # test case 2 for different yaml files

# original Function of tests
'''
 def test_calculate_sha256(file_path: Path) -> str:
    """
    calculates check sum sha256 for a file 
    
    input:file path of the file you want checksum
    output: string of checksum
    """

    hasher = hashlib.sha256()
    with open(file_path, mode="rb") as file:
        chunk_size = 8192 

        while chunk := file.read(chunk_size):
            hasher.update(chunk)

    return hasher.hexdigest()
'''

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



def test_download_stream(url: str,output_path:Path, timeout:float = 15.0 ) -> None:
    """
    Downloads data from a URL to output_path, streaming to avoid
    loading the whole file into memory. Raises on HTTP errors or timeout.

    input: url location of the data, ouput file path location,
    timeout duration for every chunk read before quitting
    output: downloaded file
    """

    temp_path = output_path.with_suffix(output_path.suffix + ".part")


    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()

            with open(temp_path, mode="wb") as file:    
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
        temp_path.rename(output_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
