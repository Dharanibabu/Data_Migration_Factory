import logging
from os.path import isfile, join

from dmf.connectors import databases
from dmf.utils.config_parsers import CSVRulebookParser
from dmf.utils.file_utils import read_yaml
from dmf.utils.logger_utils import LoggerUtils
from dmf.utils.spark_utils import SparkSessionUtils

logger = LoggerUtils().setup_logger()
log = logging.getLogger("Logger")


class ConfigManager(LoggerUtils, SparkSessionUtils):
    """
    Manages the configuration setup for job datasource and rulebook.

    This class handles the setup of job datasource configurations from provided files,
    manages rulebook configurations, sets up Spark session, and creates connectors.

    Attributes:
    - source_dfs (dict): Dictionary to store source dataframes.
    - transformed_dfs (dict): Dictionary to store transformed dataframes.
    - sources (dict): Dictionary to store source connector objects.
    - destinations (dict): Dictionary to store destination connector objects.
    - config_file_name (str): Name of the configuration file (default: "config.yml").
    - rulebook_file_name (str): Name of the rulebook file (default: "rulebook.csv").
    """

    source_dfs = {}
    transformed_dfs = {}
    sources = {}
    destinations = {}
    config_file_name = "config.yml"
    rulebook_file_name = "rulebook.csv"

    def __load_job_config(self):
        """
        Loads job datasource configuration.

        If job directory is available, it loads the config file from the directory and
        sets up Spark, source, and destination configurations.
        """
        log.info(f"Setting up job datasource configuration")
        if self.job_dir_path:
            log.info(f"Getting config file from job directory: {self.job_dir_path}")
            self.config_file_path = join(self.job_dir_path, ConfigManager.config_file_name)
        log.info(f"Config file path: {self.config_file_path}")
        self.job_config = read_yaml(file_path=self.config_file_path)
        self.spark_config = self.job_config.get("spark", None)
        self.source_config = self.job_config.get("source", [])
        self.destination_config = self.job_config.get("destination", [])
        log.info(f"Setup done for job datasource configuration")

    def __load_rulebook_config(self):
        """
        Loads job rulebook configuration.

        If job directory is available, it looks for the rulebook file and parses it
        to set up rule configurations.
        """
        log.info(f"Setting up job rulebook configuration")
        if self.job_dir_path:
            rulebook_path = join(self.job_dir_path, ConfigManager.rulebook_file_name)
            if isfile(rulebook_path):
                log.info(f"Getting rulebook file from job directory: {self.job_dir_path}")
                self.rulebook_file_path = rulebook_path
            else:
                self.rulebook_file_path, self.rule_config = None, None
                log.warning("No rulebook available in given job directory.")
        if self.rulebook_file_path:
            log.info(f"Rulebbook file path: {self.config_file_path}")
            job_type = "Lift, Transform and Shift (LTS)"
            self.rule_config = CSVRulebookParser().parse(rulebook_file=self.rulebook_file_path)
        else:
            job_type = "Lift and Shift (LS)"
        log.critical(f"Job Type: {job_type}")
        log.info(f"Setup done for Rulebook configuration")

    def __load_spark_session(self):
        """
        Sets up Spark session.
        """
        self.create_spark_session()

    def __load_connectors(self):
        """
        Function will create connector objects using self.source_config & source.destination_config
        These connector objects will be used by components in the framework.

        basically, main objective for this function to create connector objects only once and reused in other parts.
        also we can store connector specific details in the connector object itself which can be accessed later.

        """
        self.sources = {
            source["name"]: databases[source["type"]] for source in self.source_config
        }
        self.destinations = {
            destination["name"]: databases[destination["type"]]
            for destination in self.destination_config
        }

    def init_config(self, job_dir: str = None, config_file: str = None, rulebook_file: str = None):
        """
        Initiates configuration setup.

        Args:
        - job_dir (str): Path to the folder where configuration files are available.
        - config_file (str): Path to the configuration file.
        - rulebook_file (str): Path to the rulebook file (optional).
        """
        log.info(f"Job creating configurations from given configuration")

        self.job_dir_path = job_dir
        self.config_file_path = config_file
        self.rulebook_file_path = rulebook_file

        self.__load_job_config()
        self.__load_rulebook_config()
        self.__load_spark_session()
        self.__load_connectors()