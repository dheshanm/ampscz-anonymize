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
from typing import Dict, Set
import random
import json

from rich.logging import RichHandler
import pandas as pd

from anonymizer.helpers import utils

MODULE_NAME = "anonymizer_site_map"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def generate_site_id() -> str:
    """
    Generate a new site ID as a random 2-letter string.

    Returns:
        str: The generated site ID.
    """
    site_id = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=2))
    return site_id


def get_anonymized_site_id(site: str, site_map: Dict[str, str]) -> str:
    """
    Get the anonymized site ID for a given site.
    Checks the site map to ensure that the site ID is unique.

    Args:
        site (str): The original site name.
        site_map (Dict[str, str]): A dictionary mapping original site names to anonymized site IDs.

    Returns:
        str: The anonymized site ID for the given site.
    """
    if site in site_map:
        return site_map[site]

    site_id = generate_site_id()

    # make sure it's not already in the map
    while site_id in site_map.values():
        site_id = generate_site_id()

    return site_id


def get_site_map(sites: Set[str]) -> Dict[str, str]:
    """
    Generate a site map that maps original site names to anonymized site IDs.

    Args:
        sites (Set[str]): A set of original site names.

    Returns:
        Dict[str, str]: A dictionary mapping original site names to anonymized site IDs.
    """
    site_map: Dict[str, str] = {}

    for site in sites:
        anonymized_site_id = get_anonymized_site_id(site, site_map)
        site_map[site] = anonymized_site_id

    return site_map


def get_all_sites(source: Path) -> Set[str]:
    """
    Retrieves all unique site codes from a given data source.

    Args:
        source (Path): The path to the data source.

    Returns:
        Set[str]: A set of unique site codes.
    """
    df = pd.read_csv(source)
    subject_col = [c for c in df.columns if "subject" in c.lower()]

    if not subject_col:
        raise KeyError(f"Subject column not found in {source}")
    else:
        logger.info(f"Found subject column: {subject_col}")
        subject_col = subject_col[0]

    sites: Set[str] = set()
    subjects = df[subject_col].unique().tolist()
    for subject in subjects:
        try:
            site = subject[:2]
            sites.add(site)
        except TypeError:
            logger.warning(f"Invalid subject: {subject}")

    return sites


def generate_site_map(config_file: Path) -> None:
    """
    Generate a site map based on the provided configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
    config_params = utils.config(config_file, "general")
    mapping_config = utils.config(config_file, "mappings")

    mappings_root = Path(config_params["mappings_root"])

    sources_r = mapping_config["site_mapping_sources"]
    sources = [Path(s.strip()) for s in sources_r.split(",")]

    sites: Set[str] = set()
    for source in sources:
        if not source.exists():
            raise FileNotFoundError(f"Source {source} does not exist")
        logger.info(f"Reading site mapping from {source}...")

        sites_s = get_all_sites(source)
        logger.info("Sites found:")
        logger.info(sites_s)
        sites.update(sites_s)

    site_map = get_site_map(sites)
    site_map["combined"] = "combined"

    skip_site_map = bool(mapping_config["skip_site_map"])
    if skip_site_map:
        logger.info("Skipping site map generation...")

        for site in sites:
            site_map[site] = site

    mapping_dest = mappings_root / "site_mapping.json"

    logger.info(f"Writing site mapping to {mapping_dest}...")
    with open(mapping_dest, "w") as f:
        json.dump(site_map, f, indent=4)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    logger.info("Generating site maps...")
    generate_site_map(config_file)

    logger.info("Done!")
