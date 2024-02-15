"""
MSSQLConnector Module

This module contains classes and methods to connect to a Microsoft SQL Server database using PySpark.
It includes functionalities to read data from and write data to the MSSQL database, fetch table schemas,
generate compatible MSSQL schemas for PySpark DataFrames, and handle appending or overwriting data in MSSQL tables.

Classes:
- MSSQLConnector: A class for connecting to and interacting with a Microsoft SQL database in PySpark.
- MSSQLSchemaGenerator: A class that generates an MSSQL schema string by comparing given schema and a PySpark DataFrame schema.

MSSQLConnector Methods:
- read_datasource: Reads data from an MSSQL database into a PySpark DataFrame.
- read_table_schema: Retrieves the schema of a table from an MSSQL database.
- append_table: Writes a PySpark DataFrame to an MSSQL database using append mode.
- overwrite_table: Writes a PySpark DataFrame to an MSSQL database using overwrite mode.
- write_datasource: Writes the provided DataFrame to the specified database table.

MSSQLSchemaGenerator Methods:
- generate_schema: Generates an MSSQL-compatible schema based on the provided schema information.

Attributes:
- driver: The driver used for connecting to MSSQL.
- format: The format for interactions with MSSQL.
- jdbc_url_prefix: The JDBC URL prefix for MSSQL connections.
- schemas_dir: Directory path for schema files.

Usage:
1. Instantiate MSSQLConnector, set configurations, and use methods to interact with MSSQL databases.
2. Utilize MSSQLSchemaGenerator to generate MSSQL-compatible schemas for PySpark DataFrames based on provided schemas.

Dependencies:
- py4j: A bridge between Python and Java.
- pyspark: PySpark for working with Spark DataFrames.

Notes:
- Ensure necessary dependencies are installed for proper functioning of this module.
- Modify configurations and methods as needed to suit specific use cases and database requirements.
"""


import json
import logging
import os
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from dmf.connectors.base import BaseConnector
from dmf.utils.file_utils import read_json

log = logging.getLogger("Logger")


class MSSQLConnector(BaseConnector):
    """
    A class for connecting to and interacting with an Microsoft SQL database in PySpark.
    """

    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    format = "jdbc"
    connector = "MSSQLConnector"
    jdbc_url_prefix = "jdbc:sqlserver://"
    schemas_dir = Path(__file__).parents[1].joinpath("schemas")

    source_schemas = {}
    destination_schemas = {}

    def __init__(self):
        super().__init__()

    def __set_datasource_configs(self, spark_session: SparkSession, read_config: dict):
        """
        Sets configurations for the MSSQL Connector based on provided parameters.

        Args:
        - spark_session (SparkSession): The active Spark session.
        - read_config (dict): The configuration dictionary containing parameters for the connector.

        This method sets various connection parameters required for interacting with the MSSQL database,
        such as host, port, database name, URL, table name, query (if provided), username, password,
        operation mode, and an optional connection string.

        The method first initializes the Spark session and reads the configuration details passed as input.
        It checks for a profile within the configuration and updates the configurations if a profile is specified.

        Sets the following attributes based on the provided configuration:
        - spark_session: The SparkSession instance for interactions.
        - config: The read configuration dictionary.
        - profile: The profile specified in the configuration.
        - host: The host name or IP address of the MSSQL server.
        - port: The port number of the MSSQL server (default: 1521).
        - database: The name of the database in MSSQL.
        - url: The JDBC URL formed using the host, port, and database information.
        - table: The table name for operations (if specified in the configuration).
        - query: The SQL query for reading data (if specified in the configuration).
        - user: The username for authentication.
        - password: The password for authentication.
        - mode: The mode of operation (e.g., 'append', 'overwrite').
        - schema_file_name: The schema file name if mode is not 'append' (optional).

        Notes:
        - If 'profile' is specified, the method updates the configuration with profile-specific settings.
        - Default values are used for unspecified parameters like host, port, and an empty string for username/password.
        - If connection string is given, it will be used for url parameter else it will be formed using the
          provided host, port, and database details for JDBC connection.
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
            self.database = self.config.get("database")
            self.url = self.url = f"{self.jdbc_url_prefix}{self.host}:{self.port};databaseName={self.database};encrypt=true;trustServerCertificate=true;"

        self.table = self.config.get("table", None)
        self.query = self.config.get("query", None)
        self.user = self.config.get("username", "")
        self.password = self.config.get("password", "")
        self.mode = self.config.get("mode", None)

        self.schema_file_name = (
            None if self.mode == "append" else self.config.get("schema", None)
        )

    def read_datasource(self, spark_session: SparkSession, read_config: dict):
        """
        Reads data from an MSSQL database into a PySpark DataFrame.

        Args:
        - spark_session (SparkSession): The active Spark session.
        - read_config (dict): The configuration dictionary containing parameters for reading data.

        Returns:
        - DataFrame: A PySpark DataFrame containing the read data from the specified MSSQL database table or query.

        Reads the specified table or query data from the MSSQL database based on provided configurations:
        - Sets MSSQL connection configurations using '__set_datasource_configs'.
        - Logs initiation and details of the read operation.
        - Retrieves the schema of the source table using 'read_table_schema' and updates the source_schemas attribute.
        - Constructs the read_key and read_value for DataFrame reading based on query or table.
        - Executes the read operation using PySpark DataFrame reader and returns the read DataFrame.

        Notes:
        - The method uses SparkSession's read functionality to load data from the MSSQL database.
        - Logs details such as driver, URL, profile, read_key, and username for reference and troubleshooting.
        - Raises an exception if an error occurs during the read operation.
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
        except Exception as mssql_read_exception:
            log.error("Error in MSSQL Connection during read operation")
            raise mssql_read_exception

    def read_table_schema(self):
        """
        Retrieve the schema of a table from an MSSQL database.

        Returns:
        List[dict]: A list of dictionaries, each representing the schema information for a column in the table.
                    Each dictionary contains keys: 'NAME', 'DATATYPE', 'TYPE', 'LENGTH', 'PRECISION', 'SCALE', 'NULLABLE'.
                    Example:
                    [
                        {'NAME': 'num', 'DATATYPE': 'tinyint', 'TYPE': 'tinyint', 'LENGTH': '', 'PRECISION': '3', 'SCALE': '0', 'NULLABLE': 'NOT NULL'},
                        {'NAME': 'CGB_NUMMER', 'DATATYPE': 'int', 'TYPE': 'int', 'LENGTH': '', 'PRECISION': '10', 'SCALE': '0', 'NULLABLE': 'NOT NULL'},
                        {'NAME': 'VERBINDINGSNUMMER', 'DATATYPE': 'int', 'TYPE': 'int', 'LENGTH': '', 'PRECISION': '10', 'SCALE': '0', 'NULLABLE': 'NOT NULL'},
                        # ... other columns' schema details
                    ]

        Raises:
        RuntimeError: If there's a login failure or the database cannot be opened.
        """

        schema_query = f"""
                        SELECT
                            column_name AS NAME,
                            CASE
                                WHEN data_type = 'nvarchar' OR data_type = 'varchar' THEN data_type + '(' + CAST(character_maximum_length AS VARCHAR(50)) + ')'
                                WHEN data_type = 'decimal' THEN
                                    CASE
                                        WHEN numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL THEN data_type + '(' + CAST(numeric_precision AS VARCHAR(50)) + ',' + CAST(numeric_scale AS VARCHAR(50)) + ')'
                                        WHEN numeric_precision IS NOT NULL THEN data_type + '(' + CAST(numeric_precision AS VARCHAR(50)) + ')'
                                        ELSE data_type
                                    END
                                -- Add other cases for different data types if needed
                                ELSE data_type
                            END AS DATA_TYPE,
                            data_type as TYPE,
                            CAST(character_maximum_length AS VARCHAR(50)) AS LENGTH,
                            CAST(numeric_precision AS VARCHAR(50)) AS PRECISION,
                            CAST(numeric_scale AS VARCHAR(50)) AS SCALE,
                            CASE
                                WHEN is_nullable = 'YES' THEN 'NULL'
                                WHEN is_nullable = 'NO' THEN 'NOT NULL'
                                ELSE 'NULL'
                            END AS NULLABLE
                        FROM
                            information_schema.columns
                        WHERE
                            table_name = '{self.table}'
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
            schema_df = schema_df.toPandas()
            schema_list = json.loads(schema_df.to_json(orient="records"))
            if not schema_list:
                log.warning(f"No schema available for the table {self.table}")
            return schema_list
        except Py4JJavaError as java_error:
            log.error("Error in MsSQL Connection during schemas read operation")
            for err in java_error.java_exception.getStackTrace():
                if "com.microsoft.sqlserver.jdbc.SQLServerException" in str(err):
                    log.error(
                        f"Cannot open database {self.database} requested by the login. The login failed."
                    )
                    raise RuntimeError("Login failed. Cannot open database.")
            raise java_error

    def append_table(self, df: DataFrame):
        """
        Writes a DataFrame to an MSSQL database using the append mode.

        Args:
        - df (DataFrame): The PySpark DataFrame to be written to the MSSQL database.

        Writes the provided DataFrame to the MSSQL database table specified in the configurations:
        - Retrieves the schema from the table if available using 'read_table_schema'.
        - If the schema is available, generate the MSSQL compatible schema from the table using 'MSSQLSchemaGenerator'.
        - Constructs the DataFrame write operation using PySpark DataFrame writer in append mode.

        Notes:
        - The method uses SparkSession's write functionality to append data to the MSSQL database.
        - If an error occurs during the write operation, it raises an exception.
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
            log.info(f"Successfully extracted schema")
            write_schema = None
            if schema:
                log.info("Generating MSSQL Compatible schema from table schema")
                df, write_schema = MSSQLSchemaGenerator(
                    df=df,
                    config=self.config,
                    spark_session=self.spark_session,
                    schema=schema,
                ).generate_schema()
                log.info("Schema Generated for appending to the table.")

            write_df = (
                df.write.format(self.format)
                .option("driver", self.driver)
                .option("url", self.url)
                .option("dbtable", self.table)
                .option("user", self.user)
                .option("password", self.password)
                .mode(mode)
            )

            if write_schema:
                write_df.option("createTableColumnTypes", write_schema)

            write_df.save()

        except Exception as mssql_write_append_exception:
            log.error(f"An error occurred: {str(mssql_write_append_exception)}")
            raise mssql_write_append_exception

    def overwrite_table(self, df: DataFrame):
        """
        Writes a DataFrame to an MSSQL database using overwrite mode.

        Args:
        - df (DataFrame): The PySpark DataFrame to be written to the MSSQL database.

        Writes the provided DataFrame to the specified MSSQL database table in overwrite mode:
        - Retrieves the schema from the schema file if available using '__get_file_schema_dict'.
        - If the schema is available, generates MSSQL compatible the schema from the file using 'MSSQLSchemaGenerator'.
        - Constructs the DataFrame write operation using PySpark DataFrame writer in overwrite mode.
        - Saves the DataFrame to the specified MSSQL table, overwriting existing data.

        Notes:
        - The method uses SparkSession's write functionality to overwrite data in the MSSQL database.
        - If an error occurs during the write operation, it raises an exception.
        """

        mode = "overwrite"
        try:
            log.debug(f"\tDriver: {self.driver}")
            log.debug(f"\tURL: {self.url}")
            log.debug(f"\tProfile: {self.profile}")
            log.debug(f"\tTable: {self.table}")
            log.debug(f"\tUsername: {self.user}")
            log.debug(f"\tWrite Mode: {mode}")

            log.info(f"reading schema from schema file if available")
            schema = self.__get_file_schema_dict()
            write_schema = None
            if schema:
                log.info("Extracting Schema From Connector's Schema Generator.")
                df, write_schema = MSSQLSchemaGenerator(
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
                .mode(mode)
            )

            if write_schema:
                write_df.option("createTableColumnTypes", write_schema)

            write_df.save()
        except Exception as mssql_write_overwrite_exception:
            log.error(f"An error occurred: {str(mssql_write_overwrite_exception)}")
            raise mssql_write_overwrite_exception

    def write_datasource(
        self, df: DataFrame, spark_session: SparkSession, write_config: dict
    ):
        """
        Writes the provided DataFrame to the specified MSSQL database table based on the configured mode.

        Args:
        - df (DataFrame): The PySpark DataFrame to be written to the MSSQL database.
        - spark_session (SparkSession): The active Spark session.
        - write_config (dict): The configuration dictionary containing parameters for writing data.

        Writes the provided DataFrame to the specified MSSQL database table based on the configured mode:
        - Initializes the datasource configurations using '__set_datasource_configs'.
        - Handles different write modes ('append' or 'overwrite') by calling respective methods ('append_table' or 'overwrite_table').
        - Retrieves the schema of the destination table using 'read_table_schema' and updates destination_schemas.

        Notes:
        - The method uses the 'append_table' or 'overwrite_table' method based on the specified mode.
        - Raises an exception if an error occurs during the write operation.
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
        except Exception as mssql_write_exception:
            log.error("Error in MSSQL Connection during write operation")
            raise mssql_write_exception

    def __get_file_schema_dict(self):
        """
        Retrieves the schema from a JSON file based on the provided configuration.

        Forms the file path using the schemas directory and reads the schema JSON file if available.

        Returns:
        - dict: A dictionary representing the schema structure from the JSON file.

        TODO: Include an example of the schema structure and how it should be formatted in the JSON file.

        This function checks for the existence of a schema JSON file based on the provided configuration:
        - Constructs the schema file path using the schemas directory and the provided schema name or configuration name.
        - Verifies the existence of the schema file; if not found, logs a warning and returns an empty dictionary.
        - If the schema file exists, reads the JSON file and returns the schema dictionary.

        Notes:
        - This method is responsible for retrieving the schema structure from a JSON file.
        - If no schema file is found, an empty dictionary is returned.
        """

        schema_file_name = self.schema_file_name or self.config["name"] + ".json"
        schema_file_path = self.schemas_dir.joinpath(schema_file_name)
        if not os.path.exists(schema_file_path):
            log.warning("No schema file provided.")
            return []
        return read_json(schema_file_path)


class MSSQLSchemaGenerator:
    """
    Class that generates an MSSQL schema string by comparing the given schema and a PySpark DataFrame schema.

    Parameters:
    - df (DataFrame): The PySpark DataFrame.
    - config (dict): Configuration settings.
    - spark_session (SparkSession): The Spark session.
    - schema (list): The schema used for comparison.

    Methods:
    - __init__(self, df: DataFrame, config: dict, spark_session: SparkSession, schema: list):
        Initializes the MSSQLSchemaGenerator with the provided DataFrame, configuration, Spark session, and schema.

    - generate_schema(self):
        Generates the MSSQL schema by comparing the given schema with the DataFrame schema.

    Private Methods:
    - __get_df_schema_dict(self):
        Generates the schema dictionary of the DataFrame in the structure compatible with the file schema.

    - __update_df_nullability(self):
        Updates the nullability of DataFrame columns based on the file schema nullability.

    - __get_column_schema(self, column: str, type: str):
        Generates the schema for a specific column based on its type.

    Attributes:
    - df (DataFrame): The PySpark DataFrame.
    - config (dict): Configuration settings.
    - spark_session (SparkSession): The Spark session.
    - df_schema (dict): The schema dictionary representing the DataFrame's structure.
    - schema (list): The schema used for comparison.

    Notes:
    - This class is designed to facilitate the generation of an MSSQL schema using PySpark DataFrame and provided schema information.
    """

    def __init__(
        self, df: DataFrame, config: dict, spark_session: SparkSession, schema: list
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
        - This method converts certain data types to MSSQL-compatible types using a mapping dictionary.
        """
        type_mapping = {"tinyint": "tinyint", "int": "int"}
        oracle_type = type_mapping.get(type, None)
        if oracle_type:
            return f"{column} {oracle_type}"
        return ""

    def generate_schema(self):
        """
        Generates an MSSQL-compatible schema based on the provided schema information.

        Returns:
        - Tuple: The updated DataFrame with the generated schema, and a string representation of the MSSQL schema.

        Notes:
        - If the provided schema is empty or None, returns the original DataFrame and an empty schema representation.
        - Updates the DataFrame nullability based on the provided schema.
        - Generates an MSSQL-compatible schema by iterating through the provided schema.
        - Utilizes '__get_column_schema' to generate column schemas.
        """
        self.__update_df_nullability()

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