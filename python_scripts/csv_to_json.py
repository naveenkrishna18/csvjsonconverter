# csv to json conversion lambda script
# Author - Bargavi P
# github profile - https://github.com/BargaviPonram

import boto3
import csv
import json
import logging
import os

# env vars
csv_bucket = os.getenv("CSV_BUCKET_NAME")
json_bucket = os.getenv("JSON_BUCKET_NAME")

# Boto 3 configurations for s3 bucket
s3 = boto3.client('s3')

def setup_logging(log_level):
    lambda_logger = logging.getLogger('csv_to_json')
    lambda_logger.setLevel(log_level)
    return lambda_logger


logger = setup_logging("DEBUG")


def download_file_from_s3(file):
    try:
        download_path = '/tmp/input_data.csv'
        s3.download_file(csv_bucket, file, download_path)
        return download_path
    except Exception as err:
        logger.error(err)


def upload_to_s3(json_file_path, s3_output_key):
    try:
        s3.upload_file(json_file_path, json_bucket, s3_output_key)
    except Exception as err:
        logger.error(err)


def csv_to_json(file_path):
    json_list = []
    json_path = '/tmp/output_file.json'
    try:
        with open(file_path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                json_list.append(row)
        with open(json_path, 'a') as json_file:
            json_data = json.dumps(json_list, indent=2)
            json_file.write(json_data)
        return json_path
    except Exception as err:
        logger.error(err)
        return None


def lambda_handler(event, context):
    logger.debug(event)
    try:
        key = event['Records'][0]['s3']['object']['key']
        csv_file_path = download_file_from_s3(key)
        json_file_path = csv_to_json(csv_file_path)
        if json_file_path is not None:
            upload_path = key.replace("input", "output")
            s3_output_key = upload_path.replace(".csv", ".json")
            upload_to_s3(json_file_path, s3_output_key)
        else:
            raise Exception
    except Exception as err:
        logger.error(err)
