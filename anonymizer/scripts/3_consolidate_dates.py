#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "ampscz-anonymize":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import Dict
import json

from rich.logging import RichHandler
import pandas as pd

from anonymizer.helpers import utils

MODULE_NAME = "anonymizer_subject_map"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_subject_date_offset_map(source: Path) -> Dict[str, int]:
    """
    Retrieves a dictionary mapping subjects to their corresponding date offsets.

    Args:
        source (Path): The path to the source file.

    Returns:
        Dict[str, int]: A dictionary mapping subjects to their corresponding date offsets.
    """
    df = pd.read_csv(source)
    subject_date_offset_map: Dict[str, str] = {}

    subjects = df["subject"].unique().tolist()
    for subject in subjects:
        subject_date_offset_map[subject] = df[df["subject"] == subject][
            "days"
        ].tolist()[0]

    return subject_date_offset_map


def generate_subject_map(config_file: Path) -> None:
    """
    Generate a subject date mapping based on the provided configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Raises:
        FileNotFoundError: If any of the specified sources do not exist.

    """
    config_params = utils.config(config_file, "general")
    mapping_config = utils.config(config_file, "mappings")

    mappings_root = Path(config_params["mappings_root"])

    sources_r = mapping_config["sources"]
    sources = [Path(s.strip()) for s in sources_r.split(",")]

    subject_date_offset_map: Dict[str, int] = {}
    for source in sources:
        if not source.exists():
            raise FileNotFoundError(f"Source {source} does not exist")
        logger.info(f"Reading subject mapping from {source}...")

        subjects_map = get_subject_date_offset_map(source)
        subject_date_offset_map.update(subjects_map)

    subject_date_map_path = mappings_root / "subject_date_mapping.json"
    logger.info(f"Writing subject date mapping to {subject_date_map_path}...")
    with open(subject_date_map_path, "w") as f:
        json.dump(subject_date_offset_map, f, indent=4)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    logger.info("Consolidating date maps...")
    generate_subject_map(config_file)

    logger.info("Done!")
