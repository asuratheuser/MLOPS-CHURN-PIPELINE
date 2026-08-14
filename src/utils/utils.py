# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import hashlib
from pathlib import Path

import requests
import yaml


def load_yaml_config(config_path : Path ) -> dict:
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



def calculate_sha256(file_path: Path) -> str:
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


def 