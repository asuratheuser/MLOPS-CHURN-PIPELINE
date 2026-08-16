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



def download_stream(url: str,output_path:Path, timeout:float = 15.0 ) -> None:
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
