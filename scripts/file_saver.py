import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Union


class FileSaver:
    """
    Utility class for saving JSON and CSV files into a specified directory.

    Files are saved with automatically generated timestamps unless one is
    explicitly provided, and filenames follow the pattern:
    <source>_<timestamp>.<ext>
    """

    def __init__(self, base_dir: Union[str, Path], timestamp: str = None):
        """
        Initialize the FileSaver.

        Args:
            base_dir (Union[str, Path]): Base directory in which files will be saved.
            timestamp (str, optional): Optional timestamp to use in filenames.
                If not provided, the current timestamp is used in the format
                YYYY-MM-DDTHH-MM.
        """
        self.timestamp = timestamp or datetime.now().strftime("%Y-%m-%dT%H-%M")
        self.path = Path(base_dir)
        self.path.mkdir(parents=True, exist_ok=True)

    def _filename(self, source: str, ext: str) -> Path:
        """
        Construct a filename with timestamp and extension.

        Args:
            source (str): Base name of the file.
            ext (str): File extension (e.g., "json", "csv").

        Returns:
            Path: Full file path including directory and filename.
        """
        filename = f"{source}_{self.timestamp}.{ext}"
        return self.path / filename

    def save_json(self, source: str, data: Any) -> Path:
        """
        Save data to a JSON file.

        The file will be stored as <source>_<timestamp>.json.

        Args:
            source (str): Name used to generate the filename.
            data (Any): Serializable data to write to JSON.

        Returns:
            Path: Path to the written file.
        """
        path = self._filename(source, "json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return path

    def save_csv(self, source: str, rows: List[Dict[str, Any]]) -> Path:
        """
        Save a list of dictionaries as a CSV file.

        The file will be stored as <source>_<timestamp>.csv.

        Args:
            rows (List[Dict[str, Any]]): Non-empty list of dictionaries to write.
            source (str): Name used to generate the filename.

        Returns:
            Path: Path to the written file.

        Raises:
            ValueError: If `rows` is empty.
        """
        if not rows:
            raise ValueError("rows cannot be empty")

        path = self._filename(source, "csv")
        fieldnames = rows[0].keys()

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return path
