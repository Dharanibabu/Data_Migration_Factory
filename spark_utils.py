import logging
import typing
from datetime import datetime
from pathlib import Path

from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger("Logger")


class SparkSessionUtils:
    jars_dir = Path(__file__).parents[1].joinpath("jars")

    def __init_spark(
        self,
        app_name: str = "SparkApp-" + datetime.now().strftime("%Y%m%d-%H%M%S"),
        master: str = None,
        options: typing.Dict[str, str] = None,
    ):
        logger.debug(f"\tApplication Name: {app_name}")
        logger.debug(f"\tSpark Master: {master}")
        logger.debug(f"\tSpark Options: {options}")

        spark_conf = SparkConf()
        spark_conf.setAppName(app_name)
        if master:
            spark_conf.setMaster(master)

        # if option is spark.jars, load the actual jar paths as list
        if options is not None and isinstance(options, dict):
            for option, config in options.items():
                if option == "spark.jars":
                    config = [
                        str(self.jars_dir.joinpath(jar_name)) for jar_name in config
                    ]
                spark_conf.set(
                    option, ",".join(config) if isinstance(config, list) else config
                )

        self.spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    def create_spark_session(self):
        """
        Function creates spark session using config given.
        :return: spark session object.
        """
        logger.info("Initiating Spark Session ...")
        if self.spark_config:
            logger.info("Creating Spark session with given configuration")
            app_name = self.spark_config.get(
                "app_name", "SparkApp-" + datetime.now().strftime("%Y%m%d-%H%M%S")
            )
            master = self.spark_config.get("master", None)
            options = self.spark_config.get("options", None)

            self.__init_spark(app_name=app_name, master=master, options=options)
        else:
            logger.info("Creating Spark Session with default configuration")
            self.__init_spark()
