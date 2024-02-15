import os.path
from abc import ABC, abstractmethod
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from dmf.utils.file_utils import read_ini


class BaseConnector(ABC):
    """
    An abstract base class for database connectors in PySpark.

    This class defines the interface for database connectors and enforces the implementation
    of the read_datasource and write_datasource methods in derived classes.

    """

    profile_file = Path(__file__).parents[1].joinpath("configs", "profiles.ini")
    profiles = read_ini(profile_file) if os.path.exists(profile_file) else {}

    @abstractmethod
    def read_datasource(self, spark_session: SparkSession, read_config: dict):
        pass

    @abstractmethod
    def write_datasource(
        self, df: DataFrame, spark_session: SparkSession, write_config: dict
    ):
        pass

    def read_source_table_schema(self, df: DataFrame):
        pass
