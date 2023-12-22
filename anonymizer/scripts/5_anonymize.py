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
import os
from typing import Dict, Set
import warnings

from rich.logging import RichHandler
import pandas as pd

from anonymizer.helpers import utils

MODULE_NAME = "anonymizer_anonymize"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

# Hide UserWarning from pandas:
#  UserWarning: Discarding nonzero nanoseconds in conversion.

warnings.filterwarnings(
    "ignore", message="Discarding nonzero nanoseconds in conversion."
)


# Keep track of warnings:
# Prevents duplicate warnings from being printed.
WARNINGS: Set[str] = set()


def get_anonymized_date(
    row: pd.Series, col: str, subject_date_offset_map: Dict[str, int]
) -> str:
    """
    Get the anonymized date based on the subject's date offset map.

    Args:
        row (pd.Series): The row containing the subject's information.
        col (str): The column name of the date.
        subject_date_offset_map (Dict[str, int]): A dictionary mapping subject IDs to date offsets.

    Returns:
        str: The anonymized date.

    Raises:
        None
    """
    subject = row["subject_id"]
    date = row[col]

    if subject not in subject_date_offset_map:
        warn_message = f"Subject {subject} not in date offset map"
        if warn_message not in WARNINGS:
            logger.warning(warn_message)
            WARNINGS.add(warn_message)
        return date

    offset = subject_date_offset_map[subject]

    try:
        new_date = (pd.to_datetime(date) + pd.DateOffset(days=offset)).strftime(
            "%Y-%m-%d"
        )
    except ValueError:
        return date

    return new_date


def generate_anoymized_file_name(
    file_name: str, subject_map: Dict[str, str], site_map: Dict[str, str]
) -> str:
    """
    Generate an anonymized file name based on the given file name, subject map, and site map.

    Args:
        file_name (str): The original file name.
        subject_map (Dict[str, str]): A dictionary mapping original subject names to anonymized subject IDs.
        site_map (Dict[str, str]): A dictionary mapping original site names to anonymized site IDs.

    Returns:
        str: The anonymized file name.

    Raises:
        ValueError: If the file name is invalid or if the site or subject is not found in the respective maps.
    """
    # template: site-subject-*.csv
    try:
        parts = file_name.split("-")
        site = parts[0]
        subject = parts[1]
        others = parts[2:]
        others = "-".join(others)
    except IndexError:
        if file_name.endswith("metadata.csv"):
            return file_name
        raise ValueError(f"Invalid file name: {file_name}")

    if site not in site_map:
        raise ValueError(f"Invalid site: {site}")
    if subject not in subject_map:
        raise ValueError(f"Invalid subject: {subject}")

    site_id = site_map[site]
    subject_id = subject_map[subject]

    anonymized_file_name = f"{site_id}-{subject_id}-{others}"

    # logger.debug(f"{file_name} -> {anonymized_file_name}")

    return anonymized_file_name


def get_output_path(file_path: Path, data_root: Path, output_root: Path) -> Path:
    """
    Map the given file path from data root to output root.

    Args:
        file_path (Path): The path of the file.
        data_root (Path): The root directory of the data.
        output_root (Path): The root directory of the output.

    Returns:
        Path: The output path for the file.
    """
    relative_path = file_path.relative_to(data_root)
    return output_root / relative_path


def anonymize_df(
    df: pd.DataFrame,
    subject_map: Dict[str, str],
    site_map: Dict[str, str],
    subject_date_offset_map: Dict[str, int],
) -> pd.DataFrame:
    """
    Anonymizes a DataFrame by replacing sensitive information with anonymized values.

    Args:
        df (pd.DataFrame): The DataFrame to be anonymized.
        subject_map (Dict[str, str]): A dictionary mapping original subject IDs to anonymized subject IDs.
        site_map (Dict[str, str]): A dictionary mapping original site IDs to anonymized site IDs.
        subject_date_offset_map (Dict[str, int]): A dictionary mapping subject IDs to date offsets for
            anonymizing dates.

    Returns:
        pd.DataFrame: The anonymized DataFrame.
    """
    # anonymize date
    date_cols = [col for col in df.columns if "date" in col]

    for col in date_cols:
        df[col] = df.apply(
            lambda row: get_anonymized_date(row, col, subject_date_offset_map), axis=1
        )

    # anonymize subject id
    try:
        subject_col = [c for c in df.columns if "subject" in c.lower()]

        if not subject_col:
            raise KeyError("Subject column not found")
        else:
            subject_col = subject_col[0]
            df[subject_col] = df[subject_col].map(subject_map)

            # drop rows with invalid subject id
            df.dropna(subset=[subject_col], inplace=True)

    except KeyError:
        pass

    # anonymize site id
    try:
        df["site"] = df["site"].map(site_map)
    except KeyError:
        pass

    return df


def anonymize_csv(
    file_path: Path,
    subject_map: Dict[str, str],
    site_map: Dict[str, str],
    subject_date_offset_map: Dict[str, int],
    data_root: Path,
    output_root: Path,
) -> None:
    """
    Anonymizes a CSV file by applying subject, site, and date offset mappings.

    Args:
        file_path (Path): The path to the input CSV file.
        subject_map (Dict[str, str]): A dictionary mapping original subject IDs to anonymized subject IDs.
        site_map (Dict[str, str]): A dictionary mapping original site IDs to anonymized site IDs.
        subject_date_offset_map (Dict[str, int]): A dictionary mapping original subject IDs to date offsets.
        data_root (Path): The root directory of the input data.
        output_root (Path): The root directory for the output data.

    Returns:
        None
    """
    df = pd.read_csv(file_path)
    df = anonymize_df(df, subject_map, site_map, subject_date_offset_map)

    output_path = get_output_path(file_path, data_root, output_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_name = output_path.name
    try:
        anonymized_name = generate_anoymized_file_name(
            output_name, subject_map, site_map
        )
    except ValueError as e:
        logger.warning(f"Ignoring file: {file_path}: {e}")

    output_path = output_path.parent / anonymized_name
    df.to_csv(output_path, index=False)


def anonymize_data(config_file: Path) -> None:
    """
    Anonymizes data by applying mapping rules to CSV files in the specified data directory.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
    config_params = utils.config(config_file, "general")

    mappings_root = Path(config_params["mappings_root"])
    data_root = Path(config_params["data_root"])
    output_root = Path(config_params["output_root"])

    subject_map_file = mappings_root / "subject_mapping.json"
    site_map_file = mappings_root / "site_mapping.json"
    subject_date_offset_map_file = mappings_root / "subject_date_mapping.json"

    subject_map = utils.load_json(subject_map_file)
    site_map = utils.load_json(site_map_file)
    subject_date_offset_map = utils.load_json(subject_date_offset_map_file)

    with utils.get_progress_bar() as progress:
        for root, dirs, files in os.walk(data_root):
            task = progress.add_task(f"Processing {root}...", total=len(files))

            for file in files:
                progress.update(task, advance=1)
                if not file.endswith(".csv"):
                    continue

                file_path = Path(root) / file
                anonymize_csv(
                    file_path,
                    subject_map,
                    site_map,
                    subject_date_offset_map,
                    data_root,
                    output_root,
                )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    logger.info("Anonymizing data...")
    anonymize_data(config_file=config_file)

    logger.info("Done!")
