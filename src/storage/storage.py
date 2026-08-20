from pathlib import Path

from pathlib import Path
from typing import Optional


def get_project_root(
    anchor_file: str = "requirements.txt",
    start: Optional[Path] = None,
) -> Path:
    """Returns the absolute Path to the project root directory.

    The project root is defined as the nearest parent directory
    (walking upward from `start`) that contains `anchor_file`.

    Args:
        anchor_file: Name of the file used to identify the project
            root (e.g. "requirements.txt", "pyproject.toml").
        start: Path to begin searching from. Defaults to this
            module's own file location. Exposed primarily so tests
            can inject a fake starting path without patching
            `__file__`.

    Returns:
        The absolute Path to the first parent directory containing
        `anchor_file`.

    Raises:
        FileNotFoundError: If no parent directory up to the
            filesystem root contains `anchor_file`.
    """
    current = (start or Path(__file__)).resolve()

    for parent in current.parents:
        if (parent / anchor_file).exists():
            return parent

    raise FileNotFoundError(
        f"Could not locate project root: no '{anchor_file}' found in any "
        f"parent directory of {current}"
    )




def ensure_dir_exists(dir_path: Path) -> Path:
    """Guarantees that a directory path exists on disk,
    creating it if needed.
    
    input: dir_path (Path): The path to the directory
    to ensure exists
    
    output: Path object to the directory
    """
    if dir_path.exists() and not dir_path.is_dir():
        raise NotADirectoryError(
            f"Expected a directory at {dir_path}, but a file exists there instead."
        )
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path



def get_raw_data_path(filename: str = "") -> Path:

    """
    Returns the full target Path for a file inside data/raw/, ensuring the directory exists.
    
    input: filename (str): The name of the file to get the path for
    output: Path object to the file
    """

    raw_data_dir = ensure_dir_exists(get_project_root() / "data" / "raw")
    target = (raw_data_dir / filename).resolve()
    if not target.is_relative_to(raw_data_dir.resolve()):
        raise ValueError(f"filename escapes the raw data directory: {filename}")
    return target



