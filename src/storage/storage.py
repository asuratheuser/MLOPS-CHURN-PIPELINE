from pathlib import Path

def get_project_root(anchor_file: str = "requirements.txt") -> Path:
    """Returns the absolute Path object to the project root directory.
    by default, this function assumes that the project root
    is the directory containing the {anchor_file} file.
    input: None
    output: Path object to the project root directory
    """
    # old implementation (commented out)
    # project_root = Path(__file__).resolve().parents[2]


    # This function will search for the project root by looking
    #  for a {anchor_file} file in parent directories
    # can be modified to look for other files if needed (e.g. pyproject.toml, setup.py, etc.)
    # by adding an input parameter to the function (anchor_file) and checking for that file instead of requirements.txt
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / anchor_file).exists():
            return parent
    raise FileNotFoundError(
        f"Could not locate project root (no {anchor_file} found)")
    




def ensure_dir_exists(dir_path: Path) -> Path:
    """Guarantees that a directory path exists on disk, creating it if needed.
    input: dir_path (Path): The path to the directory to ensure exists
    output: Path object to the directory
    """


    return dir_path



def get_raw_data_path(filename: str = "") -> Path:
    """Returns the full target Path for a file inside data/raw/, ensuring the directory exists.
    input: filename (str): The name of the file to get the path for
    output: Path object to the file
    """



    pass