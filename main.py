"""
Main script for orchestrating the data management process.

This script utilizes argparse to handle command-line arguments for job directory,
configuration file, and rulebook file. It instantiates the Main class, which orchestrates
the data collection, transformation, and loading operations based on the provided configuration
and rulebook files.

Usage:
    $ python main_script.py --job_dir <job_directory_path> --config_file <config_file_path> [--rulebook_file <rulebook_file_path>]

Example:
    ```python
    # Instantiate Main with configuration and rulebook files
    main = Main("config_file.yml", "rulebook_file.yml")

    # Initiate the data management process
    main.start()
    ```
"""

import argparse
import logging
import os
import sys

# Ensure the parent directory of the current file is included in the Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dmf.components.collector import DataCollector
from dmf.components.loader import DataLoader
from dmf.components.transformer import DataTransformer
from dmf.utils.manager import ConfigManager

log = logging.getLogger("Logger")


class Main(ConfigManager, DataCollector, DataTransformer, DataLoader):
    """
    Main class for orchestrating the data management process.

    This class inherits from ConfigManager, DataCollector, DataTransformer, and DataLoader
    to handle configuration, data collection, transformation, and loading operations.

    Attributes:
        job_dir (str): Path to the folder where the configuration files are available.
        config_file (str): Path to the configuration file.
        rulebook_file (str): Path to the rulebook file (optional).

    Example Usage:
    ```python
    # Instantiate Main with configuration and rulebook files
    main = Main("config_file.yml", "rulebook_file.yml")

    # Initiate the data management process
    main.start()
    ```
    """

    def __init__(self, job_dir: str = None, config_file: str = None, rulebook_file: str = None):
        """
        Initialize the data management process.

        Args:
            job_dir (str): Path to the folder where the configuration files are available.
            config_file (str): Path to the configuration file.
            rulebook_file (str): Path to the rulebook file (optional).
        """
        log.info("Initiating the data management process.")
        self.init_config(job_dir=job_dir, config_file=config_file, rulebook_file=rulebook_file)
        log.info("Initialization completed.")

    def start(self):
        """
        Initiate the data management process by executing data collection, transformation,
        and loading operations sequentially.
        """
        log.info("*** Starting the Data Management Process ***")

        self.collect_data()
        self.transform_data()
        self.load_data()

        log.info("*** Data Management Process Completed ***")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_dir", help="Path to the folder where the configuration files are available")
    parser.add_argument("--config_file", help="Path to the configuration file")
    parser.add_argument("--rulebook_file", help="Path to the rulebook file (optional)")

    args = parser.parse_args()

    if not (args.job_dir or args.config_file) or (args.job_dir and args.config_file):
        arguments_rule = f"""\n\nArguments Rule:
        \t- At least one of job_dir or config_file must be provided.
        \t- But not both
        \t- rulebook_file is optional"""
        parser.error(arguments_rule)

    if args.job_dir and args.config_file:
        parser.error("Only one of job_dir or config_file should be provided, not both")

    # Instantiate Main with provided arguments and initiate the data management process
    dmf = Main(job_dir=args.job_dir, config_file=args.config_file, rulebook_file=args.rulebook_file)
    dmf.start()
