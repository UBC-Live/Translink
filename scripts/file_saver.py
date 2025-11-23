import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Union


class FileSaver:
    def __init__(self, base_dir: Union[str, Path], timestamp: str = None):
        self.timestamp = timestamp or datetime.now().strftime("%Y-%m-%dT%H-%M")
        self.path = Path(base_dir)
        self.path.mkdir(parents=True, exist_ok=True)

    def _filename(self, source: str, ext: str) -> Path:
        filename = f"{source}_{self.timestamp}.{ext}"
        return self.path / filename

    def save_json(self, source: str, data: Any) -> Path:
        path = self._filename(source, "json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return path

    def save_csv(self, rows: List[Dict[str, Any]], source: str) -> Path:
        if not rows:
            raise ValueError("rows cannot be empty")

        path = self._filename(source, "csv")
        fieldnames = rows[0].keys()

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return path
