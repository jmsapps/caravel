from __future__ import annotations

import json
import tempfile
from pathlib import Path

from .pipeline import run_to_completion


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        summary = run_to_completion(Path(directory))
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
