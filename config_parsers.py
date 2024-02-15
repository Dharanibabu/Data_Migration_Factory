"""
file consists rulebook parsers for CSV & Yaml.
These parsers will convert its configuration in a common structure which will be used in the framework.
if new extension rulebook also need to be supported, the specific Parser also should generate the common structure
"""
import csv
import json
import logging

log = logging.getLogger("Logger")


class CSVRulebookParser:
    """
    Parser to be used when extension of the rulebook file is csv.
    this will convert the csv rulebook to common structure (json structure)
    """

    def parse(self, rulebook_file: str):
        """
        Function used to parse the CSV rulebook config.
        Args:
            rulebook_file:

        Returns: common json structure to be used in the job
        """
        log.info(f"Parsing rulebook {rulebook_file} using CSVRulebookParser")
        rulebook = open(rulebook_file, "r")
        csv_reader = csv.reader(rulebook)
        header = next(csv_reader, None)
        config = []

        for row in csv_reader:
            data_dict = {header[i]: value for i, value in enumerate(row)}

            # update transformation_rules string to python obj
            # update qc_rules string to python obj

            if data_dict["transformation_rules"] in ["", None]:
                data_dict["transformation_rules"] = []
            else:
                data_dict["transformation_rules"] = json.loads(
                    data_dict["transformation_rules"]
                )

            if data_dict["qc_rules"] in ["", None]:
                data_dict["qc_rules"] = []
            else:
                data_dict["qc_rules"] = json.loads(data_dict["qc_rules"])
            config.append(data_dict)
        log.info(f"Parsing rulebook is completed.")
        return config


# todo
class YAMLRulebookParser:
    """
    Parser to be used when extension of the rulebook file is yaml.
    this will convert the yaml rulebook to common structure (json structure)
    """

    def parse(self, rulebook_file: str):
        """
        Function used to parse the Yaml rulebook config.
        Args:
            rulebook_file:

        Returns: common json structure to be used in the job
        """
        pass
