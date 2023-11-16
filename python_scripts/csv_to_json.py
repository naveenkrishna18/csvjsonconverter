import csv
import json
import logging


def setup_logging(log_level):
    lambda_logger = logging.getLogger('csv_to_json')
    lambda_logger.setLevel(log_level)
    return lambda_logger


logger = setup_logging("DEBUG")


def csv_to_json(input_path, output_path):
    json_list = []
    try:
        logger.debug("Reading csv file - {}", format(input_path))
        with open(input_path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                json_list.append(row)
        logger.debug("Writing to json file - {}", format(output_path))
        with open(output_path, 'w') as json_file:
            json_data = json.dumps(json_list, indent=2)
            json_file.write(json_data)
        return True
    except Exception as err:
        logger.error(err)
        return False

def main():
    csvFilePath = ''  # path of input file
    jsonFilePath = ''  # path of output file
    status = csv_to_json(csvFilePath, jsonFilePath)
    if status:
        logger.debug("CONVERSION SUCCESSFUL")
    else:
        logger.debug("CONVERSION FAILED")

if __name__=="__main__":
    main()