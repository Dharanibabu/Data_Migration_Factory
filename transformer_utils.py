import logging
import sys
from typing import Dict, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("Logger")


class TransformerUtils:
    """
    A utility class for performing common data transformations on PySpark DataFrames.

    Example Usage:
    ```python
    # Create a DataFrame and configure transformations using TransformerUtils.
    df = ...
    transformer = TransformerUtils()

    # Apply transformations to the DataFrame.
    transformer.rename_columns(df, {"old_column": "new_column"})
    transformer.trim_columns(my_dataframe, {"columns": ["col1", "col2"]})
    result_df = transformer.lower_columns(my_dataframe, ["col1"])
    """

    @staticmethod
    def has_column(df: DataFrame, column_name: str):
        """
        Checks if a column exists in the DataFrame.

        Args:
        - df (DataFrame): The DataFrame to check.
        - column_name (str): The name of the column to check.

        Returns:
        True if the column exists in the DataFrame, otherwise False.
        """
        try:
            df[column_name]
            return True
        except BaseException:
            return False

    @staticmethod
    def select_columns(df: DataFrame, config: list):
        """
        Function to select only the required columns from the DataFrame based on the provided list of column names.

        Args:
        - df (DataFrame): The input DataFrame.
        - columns_list (list): A list of column names to select.

        Returns:
        A DataFrame containing only the selected columns.

        Configuration Example:
        ```python
        # Define a list of columns to select
        columns_to_select = ["Name", "Age"]

        # Call the select_columns method to select specified columns
        result_df = select_columns(my_dataframe, columns_to_select)
        ```
        """
        return df.select(*config["columns"])

    def rename_columns(self, df: DataFrame, config_dict: dict):
        """
        Function to rename columns in the DataFrame using a dictionary configuration.

        Args:
        - df (DataFrame): The input DataFrame.
        - config_dict (dict): A dictionary where keys are the old column names and values are the new names.

        Returns:
        A DataFrame with renamed columns.

        Configuration Example:
        ```python
        # Define a dictionary to specify column renaming
        renaming_config = {
            "old_column_name": "new_column_name",
            "age": "age_group"
        }

        # Call the rename_columns method to rename columns
        renamed_df = rename_columns(my_dataframe, renaming_config)
        ```
        """
        for old_name, new_name in config_dict.items():
            if self.has_column(df, old_name):
                df = df.withColumnRenamed(old_name, new_name)
            else:
                print(
                    f"Column {old_name} not available to apply rename transformation."
                )
        return df

    def add_new_columns(self, df: DataFrame, columns_dict: dict):
        """
        Function to add new columns to the DataFrame using a dictionary configuration.

        Args:
        - df (DataFrame): The input DataFrame.
        - columns_dict (dict): A dictionary where keys are the new column names, and values are the default values for the new columns.

        Returns:
        A DataFrame with added columns.

        Configuration Example:
        ```python
        # Define a dictionary to specify new columns and their default values
        new_columns_config = {
            "new_column_1": 0,
            "new_column_2": "default_value"
        }

        # Call the add_new_columns method to add new columns with default values
        new_columns_df = add_new_columns(my_dataframe, new_columns_config)
        ```
        """
        for column, value in columns_dict.items():
            df = df.withColumn(column, F.lit(value))
        return df

    # TODO: Needs to be tested what is the output data type pf the column in source
    def hash_columns(self, df: DataFrame, columns_list: list):
        """
        Function to hash specified columns in a DataFrame using SHA-256 encryption.

        Args:
        - df (DataFrame): The input DataFrame.
        - columns_list (list): A list of column names to be hashed.

        Returns:
        A DataFrame with the specified columns hashed using SHA-256 encryption.

        Configuration Example:
        ```python
        # Define a list of columns to hash
        columns_to_hash = ["password", "sensitive_data"]

        # Call the hash_columns method to apply hashing to the specified columns
        hashed_df = hash_columns(my_dataframe, columns_to_hash)
        ```
        """
        for column_name in columns_list:
            if self.has_column(df, column_name):
                df = df.withColumn(column_name, F.sha2(df[column_name], 256))
            else:
                print(
                    f"Column {column_name} not available to apply hash transformation."
                )
        return df

    def join_dataframes(self, dfs: Dict[str, Union[DataFrame, DataFrame]], config):
        """
        Performs a join operation between two DataFrames based on the provided configuration.

        Args:
        - dfs (dict): A dictionary containing the DataFrames to be joined.
        - config (dict): A configuration specifying the left DataFrame, right DataFrame, join columns, and join type.

        Returns:
        A DataFrame resulting from the join operation.

        Configuration Example:
        ```python
        # Define configuration for the join operation
        join_config = {
            "left": "orders",          # Name of the left DataFrame in dfs
            "right": "customers",      # Name of the right DataFrame in dfs
            "left_column": "customer_id", # left column to be used in the condition
            "right_column": "id",       # right column to be used in the condition
            "how": "inner"             # Join type (e.g., "inner", "left", "right", "outer")
        }

        # Call the join_dataframes method to perform the join operation
        joined_df = join_dataframes(my_dataframes, join_config)
        ```
        """
        left, right = dfs["left"], dfs["right"]
        left.show()
        right.show()
        left_column, right_column = config["left_column"], config["right_column"]
        condition = (
            left_column
            if left_column == right_column
            else left[left_column] == right[right_column]
        )
        print(condition)
        how = config["how"]
        df = left.join(right, on=condition, how=how)
        return df

    def trim_columns(self, df: DataFrame, config: list):
        """
        Function to trim specified columns in a DataFrame by removing leading and trailing whitespace.

        Args:
        - df (DataFrame): The input DataFrame.
        - config (dict): A configuration specifying which columns to trim. It can include "trim_all" to trim all columns.

        Returns:
        A DataFrame with trimmed columns.

        Configuration Example (Trim Specific Columns):
        ```python
        # Define a configuration to trim specific columns
        trim_config = {
            "columns": ["name", "description"]  # List of column names to trim
        }

        # Call the trim_columns method to trim the specified list of columns
        trimmed_df = trim_columns(my_dataframe, trim_config)
        ```

        Configuration Example (Trim All Columns):
        ```python
        # Define a configuration to trim all columns in the DataFrame
        trim_config = *
        trimmed_df = trim_columns(my_dataframe, trim_config)
        ```
        """
        trim_columns = list(df.columns) if config == "*" else config

        for column_name in trim_columns:
            if self.has_column(df, column_name):
                df = df.withColumn(column_name, F.trim(F.col(column_name)))
                log.info(f"Trimming done for {column_name}")
            else:
                log.error(f"{column_name} not available to do Trim transformation")
                sys.exit(1)
        return df

    def lower_columns(self, df: DataFrame, columns_list: list):
        """
        Function to convert specified columns in a DataFrame to lowercase.

        Args:
        - df (DataFrame): The input DataFrame.
        - columns_list (list): A list of column names to convert to lowercase.

        Returns:
        A DataFrame with specified columns converted to lowercase.

        Configuration Example:
        ```python
        # Define a list of columns to convert to lowercase
        columns_to_convert = ["name", "description"]

        # Call the lower_columns method to convert specified columns to lowercase
        lowercase_df = lower_columns(my_dataframe, columns_to_convert)
        ```
        """
        for column in columns_list:
            if self.has_column(df, column):
                df = df.withColumn(column, F.lower(F.col(column)))
        return df

    def upper_columns(self, df: DataFrame, columns_list: list):
        """
        Function to convert specified columns in a DataFrame to uppercase.

        Args:
        - df (DataFrame): The input DataFrame.
        - columns_list (list): A list of column names to convert to uppercase.

        Returns:
        A DataFrame with specified columns converted to uppercase.

        Configuration Example:
        ```python
        # Define a list of columns to convert to uppercase
        columns_to_convert = ["name", "description"]

        # Call the upper_columns method to convert specified columns to uppercase
        uppercase_df = upper_columns(my_dataframe, columns_to_convert)
        ```
        """
        for column in columns_list:
            if self.has_column(df, column):
                df = df.withColumn(column, F.upper(F.col(column)))
        return df

    # TODO:
    def filter_dataframe(self, df: DataFrame, filter_list: list) -> DataFrame:
        """
        Function to filter the input dataframe based on filter expr list
        Args:
            df: Dataframe to be filtered
            filter_list: list of filter expressions

        Returns: df: Dataframe

        Configuration Example:
        ```python
        # Define a list of filter expressions
        filter_list = ["age > 30", "name != 'Alice'"]

        # Call the upper_columns method to convert specified columns to uppercase
        filtered_df = filter_dataframe(df, filter_list)
        ```
        """
        for expr in filter_list:
            df = df.filter("TYPE# == 1")
        return df

    def split_table(self, df: DataFrame, config: dict) -> DataFrame:
        """
        Function creates the dataframe by selecting the columns list given in the configuration.
        """
        df = df.select(*config["columns"])
