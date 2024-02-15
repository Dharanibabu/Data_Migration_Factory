"""
Oracle Connector Module

This module contains classes and utilities for interacting with an Oracle database using PySpark.

Classes:
- OracleConnector: A class for connecting to and managing interactions with an Oracle database.
- OracleSchemaGenerator: A class responsible for generating Oracle schema by comparing given schema and PySpark DataFrame schema.

Usage Example:
    # Create a PySpark session
    spark = SparkSession.builder.appName("OracleConnectorExample").getOrCreate()

    # Define configuration parameters using connection string
    oracle_config = {
        "connection_string": "jdbc:oracle:thin:@localhost:1521:sid",
        "username": "my_user",
        "password": "my_password",
        "table": "my_table",
        "mode": "append"
    }

    # Create an OracleConnector instance
    connector = OracleConnector()

    # Set data source configurations using the connection string
    connector.__set_datasource_configs(spark, oracle_config)

    # Read data from the Oracle table
    df = connector.read_datasource(spark, oracle_config)

    # Write data back to the Oracle table
    connector.write_datasource(df, spark, oracle_config)

    # Stop the PySpark session
    spark.stop()

    Note: Alternatively, individual parameters such as 'host', 'port', 'sid'/'service' can be set in the
    configuration dictionary instead of connection string.."""


import json
import logging
import os.path
from builtins import dict
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from dmf.connectors.base import BaseConnector
from dmf.utils.file_utils import read_json

log = logging.getLogger("Logger")


class OracleConnector(BaseConnector):
    """
    A class for connecting to and interacting with an Oracle database in PySpark.

    Class Attributes:
    - driver (str): The Oracle JDBC driver class name.
    - format (str): The format for reading and writing data using PySpark's DataFrame API.

    Init Args:
    - spark_session (pyspark.sql.SparkSession): The PySpark session to use for database operations.
    - config (dict): A dictionary containing configuration parameters for database connection and data operations.

    Example Usage:
    ```python
    from pyspark.sql import SparkSession

    # Create a PySpark session
    spark = SparkSession.builder.appName("OracleConnectorExample").getOrCreate()

    # Define configuration parameters
    oracle_config = {
        "host": "localhost",
        "port": 1521,
        "sid": "sid",
        "table": "my_table",
        "username": "my_user",
        "password": "my_password",
        "mode": "append"
    }

    # Create an OracleConnector instance
    connector = OracleConnector(spark, oracle_config)

    # Read data from the Oracle table
    df = connector.read_datasource()

    # Write data back to the Oracle table
    connector.write_datasource(df)

    # Stop the PySpark session
    spark.stop()
    ```
    """

    driver = "oracle.jdbc.OracleDriver"
    format = "jdbc"
    connector = "OracleConnector"
    jdbc_url_prefix = "jdbc:oracle:thin:@"
    schemas_dir = Path(__file__).parents[1].joinpath("schemas")

    source_schemas = {}
    destination_schemas = {}

    def __init__(self):
        """
        Initializes an OracleConnector instance which will be used for operations like read table/write tables/read table schema.

        We moved the self objects initializations to respective read/write functions.
        This will help to create the connector object only once and reuse it to all required places.
        And also this will help to keep the schemas of the tables in connector objects itself.

        """
        super().__init__()

    def __set_datasource_configs(self, spark_session: SparkSession, read_config: dict):
        """
        Sets configurations for the Oracle-19c connector based on the provided parameters.

        Args:
        - spark_session (pyspark.sql.SparkSession): The Spark session for the connection.
        - read_config (dict): A dictionary containing configuration settings for the data source.

        This method sets various connection parameters required for interacting with the Oracle database,
        such as host, port, service ID (SID), URL, table name, query (if provided), username, password,
        operation mode, and an optional connection string.

        The method first initializes the Spark session and reads the configuration details passed as input.
        It checks for a profile within the configuration and updates the configurations if a profile is specified.

        Parameters:
        - profile (str): Optional. Specifies a configuration profile if available.
        - host (str): The hostname or IP address of the Oracle database. Defaults to "localhost".
        - port (int): The port number for the Oracle database connection. Defaults to 1521.
        - sid (str): The Service ID (SID) for the Oracle database. Defaults to the value provided in the configuration,
                     falling back to an empty string if not specified.
        - connection_string (str): Optional. The pre-defined JDBC connection string. If provided, other connection parameters are ignored.
        - url (str): The constructed JDBC URL using the provided host, port, and SID.
        - table (str): The name of the table from which data will be read or written. Defaults to None.
        - query (str): The query to be executed for reading data from the database. Defaults to None.
        - user (str): The username for authentication to the Oracle database. Defaults to an empty string.
        - password (str): The password for authentication to the Oracle database. Defaults to an empty string.
        - mode (str): The mode of operation (e.g., 'append', 'overwrite') for writing data. Defaults to None.

        Note:
            This method will look for connection string. if given, it will use it for the url parameter.
            else it will form the url using host, port, sid.

        Example Usage:
        ```python
        oracle_config = {
            "connection_string": "jdbc:oracle:thin:@//localhost:1521/DMFDB",
            "table": "my_table",
            "username": "my_user",
            "password": "my_password",
            "mode": "append"
        }

        spark_session = SparkSession.builder.appName("OracleConnectorExample").getOrCreate()
        connector = OracleConnector()
        connector.__set_datasource_configs(spark_session, oracle_config)
        ```
        """
        self.spark_session = spark_session
        self.config = read_config
        self.profile = self.config.get("profile", None)
        if self.profile:
            profile_config = self.profiles.get(self.profile, {})
            self.config.update(profile_config)

        if "connection_string" in self.config:
            self.url = self.config["connection_string"]
        else:
            self.host = self.config.get("host", "localhost")
            self.port = self.config.get("port", 1521)
            self.sid = self.config.get("sid", self.config.get("service", ""))
            self.url = f"{self.jdbc_url_prefix}{self.host}:{self.port}:{self.sid}"

        self.table = self.config.get("table", None)
        self.query = self.config.get("query", None)
        self.user = self.config.get("username", "")
        self.password = self.config.get("password", "")
        self.mode = self.config.get("mode", None)

    def read_datasource(self, spark_session: SparkSession, read_config: dict):
        """
        Reads data from the specified database table or executes a query and returns the result as a DataFrame.

        Args:
        - spark_session (pyspark.sql.SparkSession): The Spark session for the connection.
        - read_config (dict): A dictionary containing configuration settings for the read operation.

        Returns:
        A DataFrame containing the data from the specified database table or query result.

        This method initiates the process of reading data from the Oracle database based on the provided read configuration.
        It sets up the data source configurations using the provided Spark session and read configuration parameters.
        It also retrieves and stores the schema of the source table in the class attribute `source_schemas`.

        Parameters:
        - read_key (str): The key used for reading data - either 'query' or 'dbtable'.
        - read_value (str): The value corresponding to the read_key - either the query string or the table name.

        NOTE:
        - The method dynamically determines whether to use a query or table name based on the presence of a query in the configuration.
        - It logs essential connection and configuration details for debugging purposes.

        Example Usage:
        ```python
        oracle_config = {
            "host": "example.com",
            "port": 1521,
            "sid": "orcl",
            "table": "my_table",
            "username": "my_user",
            "password": "my_password",
            "mode": "append"
        }

        spark_session = SparkSession.builder.appName("OracleConnectorExample").getOrCreate()
        connector = OracleConnector()
        data_frame = connector.read_datasource(spark_session, oracle_config)
        ```

        Raises:
        - Exception: If an error occurs during the read operation, it logs an error message and raises the exception.
        """
        log.info(f"Initiating {self.connector} to read {read_config['name']} source")
        self.__set_datasource_configs(
            spark_session=spark_session, read_config=read_config
        )
        self.source_schemas[self.config["name"]] = self.read_table_schema()

        read_key, read_value = (
            ("query", self.query) if self.query else ("dbtable", self.table)
        )

        log.debug(f"\tDriver: {self.driver}")
        log.debug(f"\tURL: {self.url}")
        log.debug(f"\tProfile: {self.profile}")
        log.debug(f"\t{read_key}: {read_value}")
        log.debug(f"\tUsername: {self.user}")

        try:
            read_df = (
                self.spark_session.read.format(self.format)
                .option("driver", self.driver)
                .option("url", self.url)
                .option(read_key, read_value)
                .option("user", self.user)
                .option("password", self.password)
                .load()
            )
            return read_df
        except Exception as oracle_exception:
            log.error("Error in Oracle Connection during read operation")
            raise oracle_exception

    def append_table(self, df: DataFrame):
        """
        Writes a DataFrame to an Oracle-19C database using the append mode.
        """
        mode = "append"
        try:
            log.debug(f"\tDriver: {self.driver}")
            log.debug(f"\tURL: {self.url}")
            log.debug(f"\tProfile: {self.profile}")
            log.debug(f"\tTable: {self.table}")
            log.debug(f"\tUsername: {self.user}")
            log.debug(f"\tWrite Mode: {mode}")

            log.info(f"reading schema from table: {self.table}")
            schema = self.read_table_schema()
            write_schema = None
            if schema:
                log.info("Generating Oracle-19C Compatible schema from table schema")
                df, write_schema = OracleSchemaGenerator(
                    df=df,
                    config=self.config,
                    spark_session=self.spark_session,
                    schema=schema,
                ).generate_schema()
                log.info("Schema Generated for appending to the table.")

            df.printSchema()
            df.show(5, False)

            write_df = (
                df.write.format(self.format)
                .option("driver", self.driver)
                .option("url", self.url)
                .option("dbtable", self.table)
                .option("user", self.user)
                .option("password", self.password)
                .mode(self.mode)
            )

            if write_schema:
                write_df.option("createTableColumnTypes", write_schema)

            write_df.save()

        except Exception as oracle_write_append_exception:
            log.error(f"An error occurred: {str(oracle_write_append_exception)}")
            raise oracle_write_append_exception

    def overwrite_table(self, df: DataFrame):
        """
        Writes a DataFrame to an Oracle-19C database using the append mode.
        """
        mode = "overwrite"
        try:
            log.debug(f"\tDriver: {self.driver}")
            log.debug(f"\tURL: {self.url}")
            log.debug(f"\tProfile: {self.profile}")
            log.debug(f"\tTable: {self.table}")
            log.debug(f"\tUsername: {self.user}")
            log.debug(f"\tWrite Mode: {mode}")

            log.info(f"mode: {mode}, reading schema from table")
            schema = self.__get_file_schema_dict()
            write_schema = None

            if schema:
                log.info("Extracting Schema From Connector's Schema Generator.")
                df, write_schema = OracleSchemaGenerator(
                    df=df,
                    config=self.config,
                    spark_session=self.spark_session,
                    schema=schema,
                ).generate_schema()
                log.info("Generated the schema. Using it to overwrite the dataframe.")

            write_df = (
                df.write.format(self.format)
                .option("driver", self.driver)
                .option("url", self.url)
                .option("dbtable", self.table)
                .option("user", self.user)
                .option("password", self.password)
                .mode(self.mode)
            )

            if write_schema:
                write_df.option("createTableColumnTypes", write_schema)

            write_df.save()
        except Exception as oracle_write_overwrite_exception:
            log.error(f"An error occurred: {str(oracle_write_overwrite_exception)}")
            raise oracle_write_overwrite_exception

    def write_datasource(
        self, df: DataFrame, spark_session: SparkSession, write_config: dict
    ):
        """
        Writes the provided DataFrame to the specified database table.
        """
        log.info(f"Initiating {self.connector} to write {write_config['name']} source")
        self.__set_datasource_configs(
            spark_session=spark_session, read_config=write_config
        )
        try:
            if self.config["mode"] == "append":
                self.append_table(df)
            elif self.config["mode"] == "overwrite":
                self.overwrite_table(df)

            schema = self.read_table_schema()
            self.destination_schemas[write_config["name"]] = schema
        except Exception as oracle_write_exception:
            log.error("Error in Oracle-19c Connection during write operation")
            raise oracle_write_exception

    def read_table_schema(self, table_name=None):
        """
        Fetches and retrieves the schema of a specified or default table from the Oracle 19c database.

        Args:
        - table_name (str, optional): The name of the table to fetch the schema. If not provided, uses the default table.

        Returns:
        A list containing the schema details of the specified or default table.

        This method constructs a query to obtain the schema information of the specified or default table.
        It performs a query against the 'user_tab_columns' system view to retrieve column-related details
        such as name, data type, length, precision, scale, and nullability.

        Parameters:
        - schema_query (str): SQL query to fetch table schema details.

        NOTE:
        - If 'table_name' is not provided, it uses the default table set during connector initialization.
        - The method uses the Spark session to execute the query and retrieve the schema information.
        - The retrieved schema details are formatted and converted into a list of dictionaries.

        Example Usage:
        ```python
        spark_session = SparkSession.builder.appName("OracleConnectorExample").getOrCreate()
        connector = OracleConnector()
        connector.read_table_schema("custom_table_name")  # Pass the desired table name
        ```

        Raises:
        - Exception: If an error occurs during the schema read operation, it logs an error message and raises the exception.
        """

        table_name = table_name if table_name else self.table

        schema_query = f"""
                        SELECT
                            column_name AS name,
                            CASE
                                WHEN data_type = 'VARCHAR2' THEN data_type || '(' || data_length || ')'
                                WHEN data_type = 'NUMBER' THEN
                                    CASE
                                        WHEN data_precision IS NOT NULL AND data_scale IS NOT NULL THEN data_type || '(' || data_precision || ',' || data_scale || ')'
                                        WHEN data_precision IS NOT NULL THEN data_type || '(' || data_precision || ')'
                                        ELSE data_type
                                    END
                                -- Add other cases for different data types if needed
                                ELSE data_type
                            END AS data_type,
                            data_type as type,
                            CAST(data_length AS VARCHAR(50)) AS length,
                            CAST(data_precision AS VARCHAR(50)) AS precision,
                            CAST(data_scale AS VARCHAR(50)) AS scale,
                            CASE
                                WHEN nullable = 'Y' THEN 'NULL'
                                WHEN nullable = 'N' THEN 'NOT NULL'
                                ELSE 'NULL'
                            END AS nullable
                        FROM
                            user_tab_columns
                        WHERE
                            table_name = '{table_name.upper()}'
                        """

        try:
            schema_df = (
                self.spark_session.read.format(self.format)
                .option("driver", self.driver)
                .option("url", self.url)
                .option("query", schema_query)
                .option("user", self.user)
                .option("password", self.password)
                .load()
            )
            schema_df = schema_df.fillna("")
            schema_df.show(10, False)
            schema_df = schema_df.toPandas()
            schema_list = json.loads(schema_df.to_json(orient="records"))

            if not schema_list:
                log.warning(
                    f"{table_name} not available in the source to read the schema. returning empty list."
                )
            else:
                log.info(f"{table_name} schema extracted successfully.")

            return schema_list
        except Exception as oracle_exception:
            log.error("Error in Oracle Connection during schemas read operation")
            raise oracle_exception

    def __get_file_schema_dict(self):
        """
        Function gets the schema name from config dict.
        forms the file path using schemas directory.
        reads the schema json and returns if available else returns {}
        TODO: ADD schema structure and example in this function doc.
        """
        schema_file_name = self.config.get("schema", self.config["name"] + ".json")
        schema_file_path = self.schemas_dir.joinpath(schema_file_name)
        if not os.path.exists(schema_file_path):
            log.warning(
                "No schema file provided. writing with available pyspark schema."
            )
            return {}
        return read_json(schema_file_path)


class OracleSchemaGenerator:
    """
    Class that generates an Oracle schema by comparing the given schema and a PySpark DataFrame schema.

    Parameters:
    - df (DataFrame): The PySpark DataFrame.
    - config (dict): Configuration settings.
    - spark_session (SparkSession): The Spark session.
    - schema (dict): The schema used for comparison.

    Methods:
    - __init__(self, df: DataFrame, config: dict, spark_session: SparkSession, schema: dict):
        Initializes the OracleSchemaGenerator with the provided DataFrame, configuration, Spark session, and schema.

    - generate_schema(self):
        Generates the Oracle schema by comparing the given schema with the DataFrame schema.

    Private Methods:
    - __get_df_schema_dict(self):
        Generates the schema dictionary of the DataFrame in the structure compatible with the file schema.

    - __update_df_nullability(self):
        Updates the nullability of DataFrame columns based on the file schema nullability.

    - __get_column_schema(self, column: str, type: str):
        Generates the schema for a specific column based on its type.

    Notes:
    - This class is designed to facilitate the generation of an Oracle schema using PySpark DataFrame and provided schema information.
    """

    def __init__(
        self, df: DataFrame, config: dict, spark_session: SparkSession, schema: dict
    ):
        self.config = config
        self.df = df
        self.spark_session = spark_session
        self.df_schema = self.__get_df_schema_dict()
        self.schema = schema

    def __get_df_schema_dict(self):
        """
        Function to generate the schema dictionary of the DataFrame.
        This schema dictionary reflects the structure of the file schema dictionary.

        Schema Structure:
        {
            "column_name": {
                "NAME": "column_name",
                "TYPE": <DataType>,
                "DATA_TYPE": "<data_type_name>",
                "NULLABILITY": <nullable_status>
            },
            ...
        }

        Example:
        {
            "name": {
                "NAME": "name",
                "TYPE": StringType(),
                "DATA_TYPE": "string",
                "NULLABILITY": True
            },
            "age": {
                "NAME": "age",
                "TYPE": IntegerType(),
                "DATA_TYPE": "integer",
                "NULLABILITY": False
            },
            ...
        }

        Returns:
        - dict: The schema dictionary representing the DataFrame's structure in the specified format.
        """
        df_schema = self.df.schema
        df_json = {}
        for field in df_schema.fields:
            df_json[field.name] = {
                "NAME": field.name,
                "TYPE": field.dataType,
                "DATA_TYPE": field.dataType.typeName(),
                "NULLABILITY": field.nullable,
            }
        return df_json

    def __update_df_nullability(self):
        """
        Updates the nullability of DataFrame columns based on the provided file schema nullability information.

        Notes:
        - This method adjusts the DataFrame's nullability based on the specified schema's nullability information.

        Algorithm:
        - Retrieves file schema nullability information.
        - Iterates through DataFrame schema items and modifies the nullability if specified in the file schema.
        - Generates a new DataFrame schema with adjusted nullability and updates the DataFrame.

        """
        file_schema_nullability = {
            schema["NAME"]: schema["NULLABLE"]
            for schema in self.schema
            if "NULLABLE" in schema
        }
        schema_fields = []
        for name, schema in self.df_schema.items():
            type = self.df_schema[name]["TYPE"]
            nullable = file_schema_nullability.get(
                name, self.df_schema[name]["NULLABILITY"]
            )
            field = StructField(name, type, False if nullable == "NOT NULL" else True)
            schema_fields.append(field)
        df_new_schema = StructType(schema_fields)
        self.df = self.spark_session.createDataFrame(self.df.rdd, df_new_schema)

    def __get_column_schema(self, column: str, type: str):
        """
        Generates a schema definition for a column based on the specified column name and type.

        Args:
        - column (str): The name of the column.
        - type (str): The data type of the column.

        Returns:
        - str: The schema definition for the column.

        Notes:
        - This method converts certain data types to Oracle-compatible types using a mapping dictionary.
        """
        type_mapping = {
            "VARCHAR2": "VARCHAR",
            "NUMBER": "INTEGER",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "FLOAT": "FLOAT",
            "CHAR": "CHAR",
            "NCHAR": "NCHAR",
            "CLOB": "CLOB",
            "BLOB": "BLOB",
            "BINARY_FLOAT": "BINARY_FLOAT",
            "BINARY_DOUBLE": "BINARY_DOUBLE",
            "RAW": "RAW",
            "BOOLEAN": "CHAR(1)"  # Representing boolean as CHAR(1) with 'Y'/'N' or 'T'/'F'
            # Add other mappings as needed
        }

        oracle_type = type_mapping.get(type, None)
        if oracle_type:
            return f"{column} {oracle_type}"
        return ""

    def generate_schema(self):
        """
        Generates an Oracle-compatible schema based on the provided schema information.

        Returns:
        - Tuple: The updated DataFrame with the generated schema, and a string representation of the Oracle schema.

        Notes:
        - If the provided schema is empty or None, returns the original DataFrame and None.
        - Updates the DataFrame nullability based on the provided schema.
        - Generates an Oracle-compatible schema by iterating through the provided schema.
        - Utilizes '__get_column_schema' to generate column schemas.
        """
        print(self.schema)
        if self.schema in [{}, None]:
            return self.df, None
        self.__update_df_nullability()
        print("After Nullability")
        self.df.printSchema()
        self.df.show(10, False)

        if self.config["mode"] == "append":
            return self.df, {}

        schema_list = []
        for schema in self.schema:
            column = schema["NAME"]
            type = schema["DATA_TYPE"]
            if column in self.df_schema:
                schema = self.__get_column_schema(column=column, type=type)
                if schema != "":
                    schema_list.append(schema)
        return self.df, ", ".join(schema_list)
