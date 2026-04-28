from __future__ import annotations

import logging
import os
import sys
from pathlib import Path


if __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from factory import IngestionFactory


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
        force=True,
    )

    service = IngestionFactory.create()
    service.run()


if __name__ == "__main__":
    main()
