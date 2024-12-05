import boto3
import json
from decimal import Decimal
from pathlib import Path
import logging
import sys
import os


p = Path(__file__)

dir_abs = p.parent.absolute()  

## Pass arguments for Github Action to pass file path where changes occured , access key and secret key
file_path = sys.argv[1]
access_key_id = sys.argv[2]
secret_access_key = sys.argv[3]

path_parts = file_path.split(os.path.sep)


# Find the file that got changed after input. Since the folder is named same as dynamo
directory_index = path_parts.index("input")
# Dynamo DB table name is just after the input
dynamo_table_name = path_parts[directory_index+1]

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

def put_item_ddb_table(table_name,region_name,access_key_id,secret_access_key, **kwargs):
    data = {**kwargs}
    ddb = boto3.resource('dynamodb', region_name=region_name,aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key)
    table = ddb.Table(table_name)
    response= table.put_item(Item=data)
    logger.info(f'Dynamo Entries {data}')
    return response





if __name__ == '__main__':

    logger = get_logger("Dynamo DB Entries")

    with open(f'{dir_abs}/input/ client_os_spreadsheet_ingestion/dynamo_sheet_objects.json','r') as my_file:
        items = json.load(my_file,parse_float=Decimal)


    for gsheet_entries in items:
        put_item_ddb_table(dynamo_table_name,'us-west-2',access_key_id,secret_access_key,**gsheet_entries)