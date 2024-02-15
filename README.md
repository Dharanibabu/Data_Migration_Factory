# Data Transformation Framework

PySpark based ETL framework, designed to facilitate the extract, transform and load activities across diverse systems using config-driven paradigm and a rulebook approach which empowers users to focus on transformation logic without writing manual code.

This README provides an overview of the project structure, its features, and how to use it.

## Setup

### Python
Refer `pyproject.toml` file for applicable python version. This version is expected to be available in the local machine.

### Poetry

This project uses poetry for dependency management, so install poetry by following instructions specified in https://python-poetry.org/docs/#installing-with-the-official-installer

Run `poetry install` if setting up for the first time, this will create a virtualenv for this project and install all required dependencies specified in poetry.lock file

Run `poetry shell` on terminal to activate the virtualenv to execute any command from cli

### Pre-commit

This repo is configured with pre-commit hooks to validate and format the source code for consistency. Run `pre-commit install` will setup the hooks in your local.

## Usage

The framework is packaged in the form of docker image which by default executes spark-submit command upon
running the image. The driver program is `/dmf/main.py` file which is the entry point for the spark job. It requires
two mandatory parameters `--config_file` and `--rulebook_file`. It supports all spark cli parameters.

Following is the command syntax

``` bash
docker run --net=host <docker_image_name> --master local[1] /dmf/main.py --config_file <path_to_config_yml> --rulebook_file <path_to_rulebook_file>
```

## Configuration

Spark job expects two files as input parameter one of them is the `config.yml` file which defines
spark session configuration, source and destination information from which data is to be retrived and
ingested. Below is an example

```yml
spark:
  app_name: ExampleSparkApp
  options:
    spark.driver.memory: 1g
    spark.executor.memory: 1g
    spark.jars:
      - "ojdbc8-21.1.0.0.jar"
source:
  - name: test_table
    type: oracle19c
    table: "CDB_ADVISOR_SQLPLANS"
    query: "select TASK_NAME, TASK_ID, EXECUTION_NAME, SQL_ID, PLAN_HASH_VALUE, ID from CDB_ADVISOR_SQLPLANS"
    profile: "local_dev_19c"
destination:
  - name: test_table2_destination
    type: oracle19c
    table: "demo_overwrite_table"
    target: test_table
    profile: "local_dev_19c"
    mode: "overwrite"
```

## Rulebook
The second input file is the `rulebook.csv` file which defines the mapping of source and destination columns
and their transformation logic. Below is an example

source_name | source_column | transformation_rules| destination_name   | destination_column | quality_rules
--- | --- |-----------------------------------------------------------------------------------|--------------------|--------------------|---------------|
test_table | TASK_NAME | [{""function"":""hash""},{""function"":""trim""}] | demo_append_table  | TASK_NAME_HASHED   |
test_table | | [{""function"":""add_new_column"",""config"":{""COLUMN_NAME"":""COLUMN_VALUE""}}] | demo_append_table  | NEW_COLUMN         |


## Contributing
Refer DeveloperGuidelines and PRGuidelines Doc

## Documentation

Coming Soon

## Project Structure

<details>
<summary>Project Structure: Click to expand</summary>

```bash
DMF/
│
├── collector/
│   └── collector.py
│   - Responsible for collecting data from various data sources.
│   - Defines the DataCollector class, which orchestrates data collection tasks.
│
├── connectors/
│   ├── base.py
│   │   - Contains the BaseConnector class, serving as the base class for data connectors.
│   │   - Defines abstract methods for specific data connectors to implement.
│
│   ├── oracle.py
│   │   - Handles connections and interactions with Oracle databases.
│   │   - One of many possible connector files for different data sources.
│   - Additional connector files can be added as needed.
│
├── jars/
│   │
│   ├── ...
│   │   - Directory for required Java Archive (JAR) files.
│   │   - Organized here for easy access by data connectors.
│
├── loader/
│   └── loader.py
│   - Responsible for loading transformed data into specified data destinations.
│
├── transformer/
│   ├── transformer.py
│   │   - Handles data transformation based on user-defined rules.
│   │   - Uses the TransformerUtils class from transformer_utils.py for common transformations.
│
│   ├── transformer_utils.py
│   │   - Provides utility functions for data transformation.
│   │   - Includes methods like selecting columns, renaming columns, and more.
│
├── utils/
│   ├── file_utils.py
│   │   - Contains utility functions for file operations, useful in data collection, loading, and other tasks.
│
│   ├── manager.py
│   │   - Defines the process class, the base class for your DMF job.
│   │   - Provides common functionality for data migration tasks.
│
│   ├── spark_utils.py
│   │   - Includes utility functions for managing Spark sessions, used for data processing and transformation.
│
├── main.py
│   - A script demonstrating how to initiate a data migration task using the DMF package.
│   - Can be customized to meet specific migration requirements.
│
├── config_file.yml
│   - A sample configuration file in YAML format, specifying data source and destination settings.
│   - Users can create their own configuration files based on this template.
│
├── rulebook_file.yml
│   - A sample rulebook file in YAML format, defining data transformation rules.
│   - Users can create their own rulebook files to customize data transformations.
│
└── tests/
    - Directory for adding unit tests to ensure the reliability of the DMF package (optional but recommended).
```
</details>
