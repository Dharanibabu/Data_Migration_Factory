import logging
import sys

from pyspark.sql import DataFrame

from dmf.transformers.transformer_utils import TransformerUtils

log = logging.getLogger("Logger")


class DataTransformer(TransformerUtils):
    """
    A class for performing data transformations on DataFrames using a set of transformation functions.

    Inherit from TransformerUtils to leverage common transformation functions.

    Example Usage:
    ```python
    # Define your rule configurations and DataFrames, and create a DataTransformer instance.
        *** both these are accessible from self object **
    self.rule_config = {...}
    self.dfs = {...}

    # Perform data transformations based on the rules.
    transformer.transform_data()

    # Access transformed DataFrames in transformer.dfs
    ```
    """

    def __get_source_df(self, rule) -> DataFrame:
        """
        Selects the required source DataFrame(s) from self.dfs based on the rule configuration.

        Args:
        - source_value (str): A string representing the source(s) for transformation.

        Returns:
        A dictionary of source DataFrames for transformation, target_df name

        """
        source_name = rule["source_name"]

        if len(rule["transformation_rules"]) == 1:
            function = rule["transformation_rules"][0]["function"]
            if function == "join":
                config = rule["transformation_rules"][0]["config"]
                left_df = (
                    self.transformed_dfs[config["left"]]
                    if config["left"] in self.transformed_dfs
                    else self.source_dfs[config["left"]]
                )
                right_df = (
                    self.transformed_dfs[config["right"]]
                    if config["right"] in self.transformed_dfs
                    else self.source_dfs[config["right"]]
                )
                return {"left": left_df, "right": right_df}
            if function == "select":
                config = rule["transformation_rules"][0]["config"]
                if source_name in self.transformed_dfs:
                    return self.transformed_dfs[source_name]
                elif source_name in self.source_dfs:
                    return self.source_dfs[source_name]
                else:
                    source = config["source"]
                    return (
                        self.transformed_dfs[source]
                        if source in self.transformed_dfs
                        else self.source_dfs[source]
                    )

        df = (
            self.transformed_dfs[source_name]
            if source_name in self.transformed_dfs
            else self.source_dfs[source_name]
        )
        return df

    def __apply_column_renaming_if_available(self, df, rule):
        """
        applying renaming operation on the df when available.

        when both source_column, dstination_column available in the rule,
        this function will rename the source column to dstination column

        Args:
            rule: rule from every row in the rulebook csv file
            df: df on which rename going to be applied

        Returns: df after the rename operation
        """
        source_column = rule.get("source_column", None)
        destination_column = rule.get("destination_column", None)

        if source_column not in ["", None] and destination_column not in ["", None]:
            if source_column != destination_column:
                config = {source_column: destination_column}
                log.info(
                    f"### Applying renaming transformation from column configuration"
                )
                log.debug(f"Transformation: RENAME,  config: {config}")
                updated_df = self.transformations["rename"](df, config)
                return updated_df
            else:
                log.info(
                    f"{source_column} is same as {destination_column}. Renaming is not required."
                )
        return df

    def transform(self, rule: dict):
        """
        Applies a set of data transformations to the input DataFrame based on the provided configuration.

        Args:
        - rule

        typical rule looks like::
        {
            "source_name": "test_table",
            "source_column": "",
            "transformation_rules": [
              {
                "function": "select",
                "config": [
                  "TASK_NAME",
                  "TASK_ID",
                  "EXECUTION_NAME",
                  "SQL_ID",
                  "PLAN_HASH_VALUE",
                  "ID"
                ]
              }
            ],
            "destination_column": "",
            "qc_rules": []

        }

        - source_name (required) key will be used to identify (from self.dfs) dataframe on which the transformations needs to be applied..

        - source_column (optional) is to mention a column on which the transformation will get applied.

        - dstination_column (optional) is to mention the final column name. the source column will be replaced to this name after the transformations given.

        - transformation_rules (optional) is a list of rules which needs to be performed on a column.
          when rules not given, function will apply renaming operation (if both source_column and dstination_column available)

        - qc_rules after performing all the rules, this colum is to validate the final dfs.
          (checkout documentation to know more on qc_rules, its use cases and conditions)
          TODO: needs to be updated.

        Returns:
        A DataFrame after applying the transformations specified in the rule.

        Sample Configuration Examples:
        TODO: needs to add the sample configuration.
        """
        self.transformations = {
            "select": self.select_columns,
            "rename": self.rename_columns,
            "add_new_column": self.add_new_columns,
            "hash": self.hash_columns,
            "join": self.join_dataframes,
            "trim": self.trim_columns,
            "lower": self.lower_columns,
            "upper": self.upper_columns,
            "filter": self.filter_dataframe,
            "split_table": self.split_table,
        }

        df = self.__get_source_df(rule)
        source_column = rule.get("source_column", None)
        transformations = rule.get("transformation_rules", None)

        if transformations:
            for transformation in transformations:
                function = transformation["function"]
                config = transformation.get("config", [source_column])
                log.info(f"### Applying {function.upper()} transformation")
                if function in self.transformations:
                    try:
                        log.debug(
                            f"Transformation: {function.upper()},  config: {config if config is not None else 'No Configuration'}"
                        )
                        _func = self.transformations[function.lower()]
                        df: DataFrame = _func(df, config)
                        log.info(
                            f"### {function.upper()} transformation applied successfully"
                        )
                    except BaseException:
                        log.error("Error in applying transformations. Exiting.")
                        sys.exit(1)
                else:
                    log.warning(f"transformation {function} not available, exiting.")
                    sys.exit(1)
        else:
            log.info(
                f"No transformation given for the column {source_column}, Skipping the step."
            )

        df = self.__apply_column_renaming_if_available(df, rule)

        return df

    def update_destination_columns(self):
        """
        Function selects only required columns of transformed dfs based on the rulebook configuration.
        """
        table_columns_dict = {}
        for rule in self.rule_config:
            df_name, source_column, dest_column = (
                rule["source_name"],
                rule["source_column"],
                rule["destination_column"],
            )
            dest_column = source_column if dest_column == "" else dest_column

            if df_name not in table_columns_dict:
                table_columns_dict[df_name] = []

            columns_list = (
                list(self.transformed_dfs[df_name].columns)
                if dest_column == "*"
                else [dest_column]
            )
            table_columns_dict[df_name].extend(columns_list)

        for df_name, df in self.transformed_dfs.items():
            if df_name in table_columns_dict:
                select_columns = list(
                    set(
                        [
                            column
                            for column in table_columns_dict[df_name]
                            if column in list(df.columns)
                        ]
                    )
                )
                self.transformed_dfs[df_name] = df[select_columns]

    def transform_data(self):
        """
        Initializes and performs transformations on DataFrames using a set of rules and configurations.
        """
        log.info("Initiating the Transformer Component")

        if self.rule_config:
            for rule in self.rule_config:
                log.info(f"Applying rule on source: {rule['source_name']}")
                df = self.transform(rule)
                self.transformed_dfs[rule["source_name"]] = df

            log.info(f"All transformation rules updated successfully.")
            log.info("Transformer component completed")

            self.update_destination_columns()

        else:
            log.info("No Transformations available. Skipping.")
