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
from typing import List, Dict, Set
import random
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


def generate_subject_id() -> str:
    """
    Generate a new subject ID as a random 5 digit string.

    Returns:
        str: The generated subject ID.
    """
    subject_id = "".join(random.choices("123456789", k=5))

    return subject_id


def get_anonymized_subject_id(
    subject: str, subject_map: Dict[str, str], site_map: Dict[str, str]
) -> str:
    """
    Generates an anonymized subject ID based on the given subject, subject map, and site map.

    Args:
        subject (str): The original subject ID.
        subject_map (Dict[str, str]): A dictionary mapping original subject IDs to anonymized subject IDs.
        site_map (Dict[str, str]): A dictionary mapping site IDs to anonymized site IDs.

    Returns:
        str: The anonymized subject ID.

    Raises:
        KeyError: If the site ID extracted from the subject ID is not found in the site map.
    """
    if subject in subject_map:
        return subject_map[subject]

    # firt 2 letters of subject id are site id
    site_id = subject[:2]

    # check if site id is in site map
    if site_id not in site_map:
        raise KeyError(f"Site id {site_id} not found in site map")

    # get site id from site map
    anonmymized_site_id = site_map[site_id]

    anonmymized_subject_code = anonmymized_site_id + generate_subject_id()

    # make sure it's not already in the map
    while anonmymized_subject_code in subject_map.values():
        anonmymized_subject_code = anonmymized_site_id + generate_subject_id()

    return anonmymized_subject_code


def get_subject_map(subjects: List[str], site_map: Dict[str, str]) -> Dict[str, str]:
    """
    Generates a subject map that maps original subject IDs to anonymized subject IDs.

    Args:
        subjects (List[str]): A list of original subject IDs.
        site_map (Dict[str, str]): A dictionary that maps site IDs to anonymization site IDs.

    Returns:
        Dict[str, str]: A dictionary that maps original subject IDs to anonymized subject IDs.
    """
    subject_map: Dict[str, str] = {}

    for subject in subjects:
        anonymized_subject_id = get_anonymized_subject_id(
            subject, subject_map, site_map
        )
        subject_map[subject] = anonymized_subject_id

    return subject_map


def get_all_subjects(source: Path) -> List[str]:
    """
    Retrieves all unique subjects from a CSV file.

    Args:
        source (Path): The path to the CSV file.

    Returns:
        List[str]: A list of unique subjects.

    Raises:
        KeyError: If the subject column is not found in the CSV file.
    """
    df = pd.read_csv(source)
    subject_col = [c for c in df.columns if "subject" in c.lower()]

    if not subject_col:
        raise KeyError(f"Subject column not found in {source}")
    else:
        logger.info(f"Found subject column: {subject_col}")
        subject_col = subject_col[0]

    subjects = df[subject_col].unique().tolist()

    return subjects


def get_addons_subjects(config_file: Path) -> List[str]:
    """
    Retrieves the list of subjects from the configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        List[str]: The list of subjects extracted from the configuration file.
    """
    config_params = utils.config(config_file, "addons")

    addon_subjects = config_params["subjects"]
    addon_subjects = [s.strip() for s in addon_subjects.split(",")]

    return addon_subjects


def add_sites_to_subject_map(
    subject_map: Dict[str, str], site_map: Dict[str, str]
) -> Dict[str, str]:
    """
    Adds the key-value pairs from the site_map dictionary to the subject_map dictionary.

    Args:
        subject_map (Dict[str, str]): The subject map dictionary.
        site_map (Dict[str, str]): The site map dictionary.

    Returns:
        Dict[str, str]: The updated subject map dictionary.
    """
    for site_k, site_v in site_map.items():
        subject_map[site_k] = site_v

    return subject_map


def generate_subject_map(config_file: Path) -> None:
    """
    Generate subject mapping based on the provided configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
    config_params = utils.config(config_file, "general")
    mapping_config = utils.config(config_file, "mappings")

    mappings_root = Path(config_params["mappings_root"])

    sources_r = mapping_config["subject_mapping_sources"]
    sources = [Path(s.strip()) for s in sources_r.split(",")]

    subjects: Set[str] = set()
    for source in sources:
        if not source.exists():
            raise FileNotFoundError(f"Source {source} does not exist")
        logger.info(f"Reading subject mapping from {source}...")

        subjects_s = get_all_subjects(source)
        logger.info(f"Found {len(subjects_s)} subjects")
        subjects.update(subjects_s)

    addon_subjects = get_addons_subjects(config_file)
    logger.info(f"Found {len(addon_subjects)} addon subjects")
    subjects.update(addon_subjects)

    site_map_path = mappings_root / "site_mapping.json"

    with open(site_map_path, "r") as f:
        site_map = json.load(f)

    subject_map = get_subject_map(subjects, site_map)
    subject_map["AMPSCZ"] = "AMPSCZ"
    subject_map["Prescient"] = "Prescient"
    subject_map["ProNET"] = "ProNET"
    subject_map["PRESCIENT"] = "Prescient"
    subject_map["PRONET"] = "ProNET"

    logger.info("Adding sites to subject map...")
    subject_map = add_sites_to_subject_map(subject_map, site_map)

    mapping_dest = mappings_root / "subject_mapping.json"
    logger.info(f"Writing subject mapping to {mapping_dest}...")
    with open(mapping_dest, "w") as f:
        json.dump(subject_map, f, indent=4)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    logger.info("Generating subject maps...")
    generate_subject_map(config_file)

    logger.info("Done!")
