import json
import logging

import jc
import yaml
from yaml import YAMLError

logger = logging.getLogger("DMFLogger")


def read_yaml(file_path):
    try:
        with open(file_path, "r") as file:
            logger.info(f"Reading YAML file: {file_path}")
            data = yaml.safe_load(file)
            return data
    except FileNotFoundError as file_not_found_error:
        logger.error(f"Error in Reading YAML, Exception: {file_not_found_error}")
        raise FileNotFoundError
    except YAMLError as yaml_format_error:
        logger.error(f"Error parsing {file_path}. Exception: {yaml_format_error}")
        raise YAMLError
    except BaseException as base_exception:
        logger.error(f"Error reading {file_path}, Exception: {base_exception}")
        raise BaseException


def read_ini(file_path):
    try:
        with open(file_path, "r") as file:
            logger.info(f"Reading INI file: {file_path}")
            data = jc.parse("ini", file.read())
            return data
    except FileNotFoundError as file_not_found_error:
        logger.error(f"Error in Reading INI, Exception: {file_not_found_error}")
        raise FileNotFoundError
    except Exception as read_ini_exception:
        logger.error(f"Error reading {file_path}, Exception: {read_ini_exception}")
        raise read_ini_exception


def read_json(file_path):
    try:
        with open(file_path, "r") as file:
            logger.info(f"Reading JSON file: {file_path}")
            data = json.load(file)
            return data
    except FileNotFoundError as file_not_found_error:
        logger.error(f"Error in Reading JSON, Exception: {file_not_found_error}")
        raise FileNotFoundError
    except json.JSONDecodeError as json_decode_error:
        logger.error(f"Error parsing {file_path}. Exception: {json_decode_error}")
        raise json_decode_error
    except BaseException as base_exception:
        logger.error(f"Error reading {file_path}, Exception: {base_exception}")
        raise BaseException
