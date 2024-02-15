import logging
import sys

log = logging.getLogger("Logger")


class DataLoader:
    """
    A class for loading data into different data destinations using various database connectors.

    Attributes:
        *** (All three attributes are accessed from self object) ***
    - destination_config (list): A list of destination configurations, each specifying the target and type.
    - dfs (dict): A dictionary containing DataFrames to be loaded into data destinations.
    - spark_session (pyspark.sql.SparkSession): The PySpark session for data operations.
    """

    def load_data(self):
        """
        Loads data into the specified destinations based on the provided configurations and DataFrames.
        used self object to access configurations, dataframes to be written and spark_session
        """
        log.info("Initiating the Loader component")

        for destination in self.destination_config:
            type = destination["type"]
            log.info(f"Writing datasource => name: {destination['name']}, type: {type}")

            target = destination["target"]
            df = (
                self.transformed_dfs[target]
                if target in self.transformed_dfs
                else self.source_dfs[target]
            )

            if df is None:
                log.error(
                    f"Datasource Config (target: {target}) not available to write"
                )
                sys.exit(1)

            connector = self.destinations[destination["name"]]()
            connector.write_datasource(df, self.spark_session, destination)
            log.info(f"Writing of target: {target} completed")

        log.info("Loader component completed")
