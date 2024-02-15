import logging

log = logging.getLogger("Logger")


class DataCollector:
    """
    Initiates the job to read tables using source configurations given through configuration file.
    """

    def collect_data(self):
        """
        Reads tables using configuration available in self.source_config.
        also stores the read dfs in the self.dfs as dictionary for future reference.
        Returns:
        """
        log.info("Initiating the Collector Component")

        for source in self.source_config:
            log.info(
                f"Reading datasource => name: {source['name']}, type: {source['type']}"
            )
            connector = self.sources[source["name"]]()
            df = connector.read_datasource(self.spark_session, source)
            self.source_dfs[source["name"]] = df

            log.info(f"Reading of source: {source['name']} completed")

        log.info("Collector component completed.")
